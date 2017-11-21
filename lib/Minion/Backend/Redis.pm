package Minion::Backend::Redis;
use Mojo::Base 'Minion::Backend';

use Carp 'croak';
use Digest::SHA 'sha256_base64';
use List::Util 'any';
use Mojo::IOLoop;
use Mojo::JSON qw(from_json to_json);
use Mojo::Redis2;
use Mojo::Util 'encode';
use Sort::Versions 'versioncmp';
use Sys::Hostname 'hostname';
use Time::HiRes 'time';

our $VERSION = '0.001';

has 'redis';

sub new {
  my $self = shift->SUPER::new(redis => Mojo::Redis2->new(url => shift));

  my $redis_version = $self->redis->backend->info('server')->{redis_version};
  croak 'Redis Server 2.8.0 or later is required'
    if versioncmp($redis_version, '2.8.0') == -1;

  return $self;
}

sub broadcast {
  my ($self, $command, $args, $ids) = (shift, shift, shift || [], shift || []);
  my $item = to_json([$command, @$args]);
  my %worker_ids = map { ($_ => 1) } @{$self->redis->smembers('minion.workers')};
  $ids = @$ids ? [grep { exists $worker_ids{$_} } @$ids] : [keys %worker_ids];
  my $tx = $self->redis->multi;
  $tx->rpush("minion.worker.$_.inbox", $item) for @$ids;
  $tx->exec;
  return !!@$ids;
}

sub dequeue {
  my ($self, $id, $wait, $options) = @_;

  if ((my $job = $self->_try($id, $options))) { return $job }
  return undef if Mojo::IOLoop->is_running;

  my $redis = $self->redis;
  my $timer = Mojo::IOLoop->timer($wait => sub {
    $redis->unsubscribe('message')
      ->unsubscribe(['minion.job'], sub { Mojo::IOLoop->stop });
  });
  $redis->subscribe(['minion.job'], sub {
    my ($redis, $err, $res) = @_;
    $redis->on(message => sub {
      my ($redis, $message, $channel) = @_;
      if ($channel eq 'minion.job') {
        Mojo::IOLoop->remove($timer);
        $redis->unsubscribe('message')
          ->unsubscribe(['minion.job'], sub { Mojo::IOLoop->stop });
      }
    });
  });
  Mojo::IOLoop->start;

  return $self->_try($id, $options);
}

sub enqueue {
  my ($self, $task, $args, $options) = (shift, shift, shift || [], shift || {});

  my $id = $self->redis->incr('minion.last_job_id');

  my %notes = %{$options->{notes} || {}};
  $_ = to_json($_) for values %notes;
  my $parents = $options->{parents} || [];
  my $queue = $options->{queue} // 'default';
  my $attempts = $options->{attempts} // 1;
  my $priority = $options->{priority} // 0;
  my $now = time;
  my $delayed = $now + ($options->{delay} // 0);

  my $tx = $self->redis->multi;
  $tx->hmset("minion.job.$id",
    id       => $id,
    args     => to_json($args),
    attempts => $attempts,
    created  => $now,
    delayed  => $delayed,
    parents  => to_json($parents),
    priority => $priority,
    queue    => $queue,
    retries  => 0,
    state    => 'inactive',
    task     => $task,
  );

  $tx->hmset("minion.job.$id.notes", %notes) if %notes;
  $tx->sadd("minion.job.$id.parents", @$parents) if @$parents;
  $tx->sadd("minion.job.$_.children", $id) for @$parents;

  $tx->sadd("minion.job_queue.$queue", $id);
  $tx->sadd('minion.job_state.inactive', $id);
  $tx->sadd('minion.job_state.inactive,active,failed', $id);
  $tx->sadd("minion.job_task.$task", $id);
  $tx->sadd('minion.jobs', $id);

  my $alphaid = sprintf '%012d', $id;
  $tx->zadd("minion.inactive_job_queue.$queue", (0-$priority) => $alphaid);
  $tx->zadd("minion.inactive_job_task.$task", 0 => $alphaid);
  $tx->zadd('minion.inactive_job_delayed', $delayed => $id);

  $tx->exec;

  $self->_notify_job if $delayed <= $now;

  return $id;
}

sub fail_job   { shift->_update(1, @_) }
sub finish_job { shift->_update(0, @_) }

sub list_jobs {
  my ($self, $offset, $limit, $options) = @_;

  my @sets = ('minion.jobs', map { "minion.job_$_.$options->{$_}" }
    grep { defined $options->{$_} } qw(queue state task));
  if (defined(my $ids = $options->{ids})) {
    my $tx = $self->redis->multi;
    my $key = 'minion.temp.jobs.' . join(',', @$ids);
    $tx->del($key);
    $tx->sadd($key, @$ids) if @$ids;
    $tx->expire($key, 60);
    $tx->exec;
    push @sets, $key;
  }
  my @job_ids = sort { $b <=> $a } @{$self->redis->sinter(@sets)};
  my $total = @job_ids;

  @job_ids = () if $offset >= @job_ids;
  splice @job_ids, 0, $offset if $offset < @job_ids;
  splice @job_ids, $limit if $limit < @job_ids;

  my @jobs;
  foreach my $id (@job_ids) {
    my %job_info = @{$self->redis->hgetall("minion.job.$id")};

    my $children = $self->redis->smembers("minion.job.$id.children");

    my %notes = @{$self->redis->hgetall("minion.job.$id.notes")};
    $_ = from_json($_) for values %notes;

    push @jobs, {
      id       => $job_info{id},
      args     => from_json($job_info{args} // 'null'),
      attempts => $job_info{attempts},
      children => $children,
      created  => $job_info{created},
      delayed  => $job_info{delayed},
      finished => $job_info{finished},
      notes    => \%notes,
      parents  => from_json($job_info{parents} // 'null'),
      priority => $job_info{priority},
      queue    => $job_info{queue},
      result   => from_json($job_info{result} // 'null'),
      retried  => $job_info{retried},
      retries  => $job_info{retries},
      started  => $job_info{started},
      state    => $job_info{state},
      task     => $job_info{task},
      worker   => $job_info{worker},
    };
  }
  return {jobs => \@jobs, total => $total};
}

sub list_workers {
  my ($self, $offset, $limit, $options) = @_;

  my @worker_ids = sort { $b <=> $a } @{$self->redis->smembers('minion.workers')};
  my $total = @worker_ids;

  @worker_ids = () if $offset >= @worker_ids;
  splice @worker_ids, 0, $offset if $offset < @worker_ids;
  splice @worker_ids, $limit if $limit < @worker_ids;

  my @workers;
  foreach my $id (@worker_ids) {
    my %worker_info = @{$self->redis->hgetall("minion.worker.$id")};

    my $notified = $self->redis->zscore('minion.worker_notified', $id);
    my $jobs = $self->redis->sinter("minion.worker.$id.jobs", 'minion.job_state.active');

    push @workers, {
      id       => $worker_info{id},
      notified => $notified,
      jobs     => $jobs,
      host     => $worker_info{host},
      pid      => $worker_info{pid},
      status   => from_json($worker_info{status} // 'null'),
      started  => $worker_info{started},
    };
  }

  return {total => $total, workers => \@workers};
}

sub lock {
  my ($self, $name, $duration, $options) = (shift, shift, shift, shift // {});
  $self->redis->zremrangebyscore("minion.lock.$name", '-inf', '(' . time);
  my $tx = $self->redis->multi;
  $tx->watch("minion.lock.$name");
  my $locks = $self->redis->zcard("minion.lock.$name");
  return !!0 if $locks >= ($options->{limit} || 1);
  if (defined $duration and $duration > 0) {
    my $lock_id = $self->redis->incr('minion.last_lock_id');
    $tx->zadd("minion.lock.$name", (time + $duration) => $lock_id);
    $tx->exec;
  }
  return !!1;
}

sub note {
  my ($self, $id, $key, $value) = @_;
  my $tx = $self->redis->multi;
  $tx->watch("minion.job.$id");
  return !!0 unless $self->redis->exists("minion.job.$id");
  $tx->hset("minion.job.$id.notes", $key => to_json($value));
  $tx->exec;
  return !!1;
}

sub receive {
  my ($self, $id) = @_;
  my $tx = $self->redis->multi;
  $tx->watch("minion.worker.$id.inbox");
  my $items = $self->redis->lrange("minion.worker.$id.inbox", 0, -1);
  $tx->del("minion.worker.$id.inbox");
  $tx->exec;
  return [map { from_json($_) } @$items];
}

sub register_worker {
  my ($self, $id, $options) = (shift, shift, shift || {});

  $id //= $self->redis->incr('minion.last_worker_id');

  my $now = time;

  my $tx = $self->redis->multi;
  $tx->hmset("minion.worker.$id",
    id       => $id,
    status   => to_json($options->{status} // {}),
  );
  $tx->hsetnx("minion.worker.$id", host    => $self->{host} //= hostname);
  $tx->hsetnx("minion.worker.$id", pid     => $$);
  $tx->hsetnx("minion.worker.$id", started => $now);
  $tx->zadd('minion.worker_notified', $now => $id);
  $tx->sadd('minion.workers', $id);
  $tx->exec;

  return $id;
}

sub remove_job {
  my ($self, $id) = @_;

  my $tx = $self->redis->multi;
  $tx->watch("minion.job.$id");

  my ($queue, $state, $task, $worker) =
    @{$self->redis->hmget("minion.job.$id", qw(queue state task worker))};
  return !!0 unless defined $state and
    ($state eq 'inactive' or $state eq 'failed' or $state eq 'finished');

  _delete_job($tx, $id, $queue, $state, $task, $worker);
  $tx->exec;

  return 1;
}

sub repair {
  my $self = shift;

  # Workers without heartbeat
  my $redis  = $self->redis;
  my $minion = $self->minion;
  my $tx = $redis->multi;
  $tx->watch('minion.worker_notified');
  my $missing = $redis->zrangebyscore('minion.worker_notified',
    '-inf', '(' . (time - $minion->missing_after));
  _delete_worker($tx, $_) for @$missing;
  $tx->exec;

  # Jobs with missing worker (can be retried)
  $tx = $redis->multi;
  $tx->watch('minion.jobs_missing_worker');
  my $orphaned_jobs = $redis->sinter('minion.job_state.active',
    'minion.jobs_missing_worker');
  $tx->del('minion.jobs_missing_worker');
  $tx->exec;

  foreach my $id (@$orphaned_jobs) {
    my $retries = $redis->hget("minion.job.$id", 'retries');
    $self->fail_job($id, $retries, 'Worker went away');
  }

  # Old jobs with no unresolved dependencies
  my $old_jobs = $redis->zrangebyscore('minion.job_finished',
    '-inf', '(' . (time - $minion->remove_after));
  foreach my $id (@$old_jobs) {
    my $tx = $redis->multi;
    $tx->watch("minion.job.$id", "minion.job.$id.children");
    my ($queue, $state, $task, $worker) =
      @{$redis->hmget("minion.job.$id", qw(queue state task worker))};
    next if @{$redis->sdiff("minion.job.$id.children", 'minion.job_state.finished')};
    _delete_job($tx, $id, $queue, $state, $task, $worker);
    $tx->exec;
  }
}

sub reset {
  my ($self) = @_;
  my $tx = $self->redis->multi;
  $tx->watch('minion.jobs', 'minion.workers');
  my $keys = $self->redis->keys('minion.*');
  $tx->del(@$keys) if @$keys;
  $tx->exec;
}

sub retry_job {
  my ($self, $id, $retries, $options) = (shift, shift, shift, shift || {});

  my $now = time;
  my %set;
  $set{attempts} = $options->{attempts} if defined $options->{attempts};
  $set{delayed} = my $delayed = $now + ($options->{delay} // 0);
  $set{priority} = $options->{priority} if defined $options->{priority};
  $set{retried} = $now;

  my $tx = $self->redis->multi;
  $tx->watch("minion.job.$id");

  my ($curr_queue, $curr_priority, $curr_retries, $curr_state, $task) =
    @{$self->redis->hmget("minion.job.$id",
    qw(queue priority retries state task))};
  return !!0 unless defined $curr_retries and $curr_retries == $retries;

  $tx->hmset("minion.job.$id", %set);
  $tx->hincrby("minion.job.$id", retries => 1);

  my $alphaid = sprintf '%012d', $id;
  if (defined $options->{queue}) {
    $tx->hset("minion.job.$id", queue => $options->{queue});
    $tx->srem("minion.job_queue.$curr_queue", $id);
    $tx->sadd("minion.job_queue.$options->{queue}", $id);
    $tx->zrem("minion.inactive_job_queue.$curr_queue", $alphaid);
  }

  $tx->hset("minion.job.$id", state => 'inactive');
  $tx->srem("minion.job_state.$curr_state", $id);
  $tx->sadd('minion.job_state.inactive', $id);
  $tx->sadd('minion.job_state.inactive,active,failed', $id);

  my $priority = $options->{priority} // $curr_priority;
  my $queue = $options->{queue} // $curr_queue;
  $tx->zadd("minion.inactive_job_queue.$queue", (0-$priority) => $alphaid);
  $tx->zadd("minion.inactive_job_task.$task", 0 => $alphaid);
  $tx->zadd('minion.inactive_job_delayed', $delayed => $id);

  $tx->exec;

  $self->_notify_job if $delayed <= $now;

  return 1;
}

sub stats {
  my $self = shift;

  my %stats;
  $stats{inactive_jobs} = $self->redis->scard('minion.job_state.inactive');
  $stats{active_jobs} = $self->redis->scard('minion.job_state.active');
  $stats{failed_jobs} = $self->redis->scard('minion.job_state.failed');
  $stats{finished_jobs} = $self->redis->scard('minion.job_state.finished');
  $stats{delayed_jobs} = $self->redis->zcount('minion.inactive_job_delayed', time, '+inf');
  $stats{active_workers} = 0;
  foreach my $id (@{$self->redis->smembers('minion.workers')}) {
    $stats{active_workers}++
      if @{$self->redis->sinter('minion.job_state.active', "minion.worker.$id.jobs")};
  }
  $stats{enqueued_jobs} = $self->redis->get('minion.last_job_id') // 0;
  $stats{inactive_workers} = $self->redis->scard('minion.workers') - $stats{active_workers};
  
  $stats{uptime} = $self->redis->backend->info('server')->{uptime_in_seconds};

  return \%stats;
}

sub unlock {
  my ($self, $name) = @_;
  my $tx = $self->redis->multi;
  $tx->zremrangebyscore("minion.lock.$name", '-inf', '(' . time);
  $tx->zremrangebyrank("minion.lock.$name", 0, 0);
  my $res = $tx->exec;
  return !!$res->[1];
}

sub unregister_worker {
  my ($self, $id) = @_;
  my $tx = $self->redis->multi;
  _delete_worker($tx, $id);
  $tx->exec;
}

sub _delete_job {
  my ($redis, $id, $queue, $state, $task, $worker) = @_;
  $redis->del("minion.job.$id", "minion.job.$id.notes",
    "minion.job.$id.parents", "minion.job.$id.children");
  $redis->srem("minion.job_queue.$queue", $id);
  $redis->srem("minion.job_state.$state", $id);
  $redis->srem("minion.job_task.$task", $id);
  $redis->srem('minion.job_state.inactive,active,failed', $id);
  $redis->srem('minion.jobs', $id);
  my $alphaid = sprintf '%012d', $id;
  $redis->zrem("minion.inactive_job_queue.$queue", $alphaid);
  $redis->zrem("minion.inactive_job_task.$task", $alphaid);
  $redis->zrem('minion.inactive_job_delayed', $id);
  $redis->zrem('minion.job_finished', $id);
  $redis->srem("minion.worker.$worker.jobs", $id) if defined $worker;
}

sub _delete_worker {
  my ($redis, $id) = @_;
  $redis->sunionstore('minion.jobs_missing_worker',
    'minion.jobs_missing_worker', "minion.worker.$id.jobs");
  $redis->del("minion.worker.$id", "minion.worker.$id.inbox",
    "minion.worker.$id.jobs");
  $redis->srem('minion.workers', $id);
  $redis->zrem('minion.worker_notified', $id);
}

sub _notify_job { shift->redis->publish('minion.job' => '') }

sub _try {
  my ($self, $id, $options) = @_;

  my $queues = $options->{queues} || ['default'];
  my $tasks = [keys %{$self->minion->tasks}];

  my $job;
  my $job_tx = $self->redis->multi;
  my $now = time;
  if (defined $options->{id}) {
    $job_tx->watch("minion.job.$options->{id}"); # ensure job isn't taken by someone else
    my ($queue, $task) =
      @{$self->redis->hmget("minion.job.$options->{id}", qw(queue task))};
    if (defined $task and exists $self->minion->tasks->{$task}
        and defined $queue and (any { $_ eq $queue } @$queues)) {
      $job = $self->_try_job($options->{id}, $now);
    }
  } else {
    my $queue_hash = sha256_base64(encode 'UTF-8', join(',', @$queues));
    my $queue_key = "minion.temp.queues.$queue_hash";
    my $task_hash = sha256_base64(encode 'UTF-8', join(',', @$tasks));
    my $task_key = "minion.temp.tasks.$task_hash";

    my $tx = $self->redis->multi;
    $tx->del($queue_key);
    $tx->zunionstore($queue_key, scalar(@$queues),
      map { "minion.inactive_job_queue.$_" } @$queues) if @$queues;
    $tx->expire($queue_key, 60);
    $tx->del($task_key);
    $tx->zunionstore($task_key, scalar(@$tasks),
      map { "minion.inactive_job_task.$_" } @$tasks) if @$tasks;
    $tx->expire($task_key, 60);

    my $priority_hash = sha256_base64(join '$', $queue_hash, $task_hash, $$, $now);
    my $priority_key = "minion.temp.inactive_jobs.$priority_hash";
    $tx->del($priority_key);
    $tx->zinterstore($priority_key, 2, $queue_key, $task_key, WEIGHTS => 1, 0);
    $tx->expire($priority_key, 60);

    $tx->exec;

    my $i = 0;
    while (my @check = @{$self->redis->zrangebyscore($priority_key,
      '-inf', '+inf', LIMIT => $i, 1)}) {
      my $check_id = 0+$check[0];
      $job_tx->watch("minion.job.$check_id"); # ensure job isn't taken by someone else
      $job = $self->_try_job($check_id, $now);
      last if $job;
    } continue {
      $job_tx->discard;
      $job_tx = $self->redis->multi;
      $i++;
    }
  }

  return undef unless defined $job;

  $job_tx->hmset("minion.job.$job->{id}",
    started => time,
    state   => 'active',
    worker  => $id,
  );
  $job_tx->srem('minion.job_state.inactive', $job->{id});
  $job_tx->sadd('minion.job_state.active', $job->{id});
  $job_tx->srem("minion.worker.$job->{worker}.jobs", $job->{id}) if defined $job->{worker};
  $job_tx->sadd("minion.worker.$id.jobs", $job->{id});
  my $alphaid = sprintf '%012d', $job->{id};
  $job_tx->zrem("minion.inactive_job_queue.$job->{queue}", $alphaid);
  $job_tx->zrem("minion.inactive_job_task.$job->{task}", $alphaid);
  $job_tx->zrem('minion.inactive_job_delayed', $job->{id});
  $job_tx->exec;

  return {
    id      => $job->{id},
    args    => from_json($job->{args} // 'null'),
    retries => $job->{retries},
    task    => $job->{task},
  };
}

sub _try_job {
  my ($self, $id, $now) = @_;
  my ($state, $delayed) =
    @{$self->redis->hmget("minion.job.$id", qw(state delayed))};
  return undef unless defined $state and $state eq 'inactive'
    and defined $delayed and $delayed <= $now;
  my $pending = @{$self->redis->sinter("minion.job.$id.parents",
    'minion.job_state.inactive,active,failed')};
  return undef if $pending;
  my %job;
  @job{qw(id args queue retries task worker)} =
    @{$self->redis->hmget("minion.job.$id",
    qw(id args queue retries task worker))};
  return \%job;
}

sub _update {
  my ($self, $fail, $id, $retries, $result) = @_;

  my $state = $fail ? 'failed' : 'finished';
  my $tx = $self->redis->multi;
  $tx->watch("minion.job.$id");
  my ($attempts, $curr_retries, $curr_state) =
    @{$self->redis->hmget("minion.job.$id", qw(attempts retries state))};
  return undef unless defined $curr_retries and $curr_retries == $retries
    and defined $curr_state and $curr_state eq 'active';
  my $now = time;
  $tx->hmset("minion.job.$id",
    finished => $now,
    result   => to_json($result),
    state    => $state,
  );
  $tx->srem('minion.job_state.active', $id);
  $tx->srem('minion.job_state.inactive,active,failed', $id) unless $fail;
  $tx->sadd("minion.job_state.$state", $id);
  $tx->zadd('minion.job_finished', $now => $id) unless $fail;
  $tx->exec;

  return 1 if !$fail || $attempts == 1;
  return 1 if $retries >= ($attempts - 1);
  my $delay = $self->minion->backoff->($retries);
  return $self->retry_job($id, $retries, {delay => $delay});
}

1;

=head1 NAME

Minion::Backend::Redis - Redis backend for Minion job queue

=head1 SYNOPSIS

  use Minion::Backend::Redis;
  my $backend = Minion::Backend::Redis->new('redis://127.0.0.1:6379/5');

  # Minion
  use Minion;
  my $minion = Minion->new(Redis => 'redis://127.0.0.1:6379');

  # Mojolicious (via Mojolicious::Plugin::Minion)
  $self->plugin(Minion => { Redis => 'redis://127.0.0.1:6379/2' });

  # Mojolicious::Lite (via Mojolicious::Plugin::Minion)
  plugin Minion => { Redis => 'redis://x:s3cret@127.0.0.1:6379' };

=head1 DESCRIPTION

L<Minion::Backend::Redis> is a backend for L<Minion> based on L<Mojo::Redis2>.
Note that L<Redis Server|https://redis.io/download> version C<2.8.0> or newer
is required to use this backend.

=head1 ATTRIBUTES

L<Minion::Backend::Redis> inherits all attributes from L<Minion::Backend> and
implements the following new ones.

=head2 redis

  my $redis = $backend->redis;
  $backend  = $backend->redis(Mojo::Redis2->new);

L<Mojo::Redis2> object used to store all data.

=head1 METHODS

L<Minion::Backend::Redis> inherits all methods from L<Minion::Backend> and
implements the following new ones.

=head2 new

  my $backend = Minion::Backend::Redis->new;
  my $backend = Minion::Backend::Redis->new('redis://x:s3cret@localhost:6379/5');

Construct a new L<Minion::Backend::Redis> object.

=head2 broadcast

  my $bool = $backend->broadcast('some_command');
  my $bool = $backend->broadcast('some_command', [@args]);
  my $bool = $backend->broadcast('some_command', [@args], [$id1, $id2, $id3]);

Broadcast remote control command to one or more workers.

=head2 dequeue

  my $job_info = $backend->dequeue($worker_id, 0.5);
  my $job_info = $backend->dequeue($worker_id, 0.5, {queues => ['important']});

Wait a given amount of time in seconds for a job, dequeue it and transition
from C<inactive> to C<active> state, or return C<undef> if queues were empty.

These options are currently available:

=over 2

=item id

  id => '10023'

Dequeue a specific job.

=item queues

  queues => ['important']

One or more queues to dequeue jobs from, defaults to C<default>.

=back

These fields are currently available:

=over 2

=item args

  args => ['foo', 'bar']

Job arguments.

=item id

  id => '10023'

Job ID.

=item retries

  retries => 3

Number of times job has been retried.

=item task

  task => 'foo'

Task name.

=back

=head2 enqueue

  my $job_id = $backend->enqueue('foo');
  my $job_id = $backend->enqueue(foo => [@args]);
  my $job_id = $backend->enqueue(foo => [@args] => {priority => 1});

Enqueue a new job with C<inactive> state.

These options are currently available:

=over 2

=item attempts

  attempts => 25

Number of times performing this job will be attempted, with a delay based on
L<Minion/"backoff"> after the first attempt, defaults to C<1>.

=item delay

  delay => 10

Delay job for this many seconds (from now).

=item notes

  notes => {foo => 'bar', baz => [1, 2, 3]}

Hash reference with arbitrary metadata for this job.

=item parents

  parents => [$id1, $id2, $id3]

One or more existing jobs this job depends on, and that need to have
transitioned to the state C<finished> before it can be processed.

=item priority

  priority => 5

Job priority, defaults to C<0>. Jobs with a higher priority get performed first.

=item queue

  queue => 'important'

Queue to put job in, defaults to C<default>.

=back

=head2 fail_job

  my $bool = $backend->fail_job($job_id, $retries);
  my $bool = $backend->fail_job($job_id, $retries, 'Something went wrong!');
  my $bool = $backend->fail_job(
    $job_id, $retries, {msg => 'Something went wrong!'});

Transition from C<active> to C<failed> state, and if there are attempts
remaining, transition back to C<inactive> with an exponentially increasing
delay based on L<Minion/"backoff">.

=head2 finish_job

  my $bool = $backend->finish_job($job_id, $retries);
  my $bool = $backend->finish_job($job_id, $retries, 'All went well!');
  my $bool = $backend->finish_job($job_id, $retries, {msg => 'All went well!'});

Transition from C<active> to C<finished> state.

=head2 list_jobs

  my $results = $backend->list_jobs($offset, $limit);
  my $results = $backend->list_jobs($offset, $limit, {state => 'inactive'});

Returns the information about jobs in batches.

  # Check job state
  my $results = $backend->list_jobs(0, 1, {ids => [$job_id]});
  my $state = $results->{jobs}[0]{state};

  # Get job result
  my $results = $backend->list_jobs(0, 1, {ids => [$job_id]});
  my $result = $results->{jobs}[0]{result};

These options are currently available:

=over 2

=item ids

  ids => ['23', '24']

List only jobs with these ids.

=item queue

  queue => 'important'

List only jobs in this queue.

=item state

  state => 'inactive'

List only jobs in this state.

=item task

  task => 'test'

List only jobs for this task.

=back

These fields are currently available:

=over 2

=item args

  args => ['foo', 'bar']

Job arguments.

=item attempts

  attempts => 25

Number of times performing this job will be attempted.

=item children

  children => ['10026', '10027', '10028']

Jobs depending on this job.

=item created

  created => 784111777

Epoch time job was created.

=item delayed

  delayed => 784111777

Epoch time job was delayed to.

=item finished

  finished => 784111777

Epoch time job was finished.

=item notes

  notes => {foo => 'bar', baz => [1, 2, 3]}

Hash reference with arbitrary metadata for this job.

=item parents

  parents => ['10023', '10024', '10025']

Jobs this job depends on.

=item priority

  priority => 3

Job priority.

=item queue

  queue => 'important'

Queue name.

=item result

  result => 'All went well!'

Job result.

=item retried

  retried => 784111777

Epoch time job has been retried.

=item retries

  retries => 3

Number of times job has been retried.

=item started

  started => 784111777

Epoch time job was started.

=item state

  state => 'inactive'

Current job state, usually C<active>, C<failed>, C<finished> or C<inactive>.

=item task

  task => 'foo'

Task name.

=item worker

  worker => '154'

Id of worker that is processing the job.

=back

=head2 list_workers

  my $results = $backend->list_workers($offset, $limit);
  my $results = $backend->list_workers($offset, $limit, {ids => [23]});

Returns information about workers in batches.

  # Check worker host
  my $results = $backend->list_workers(0, 1, {ids => [$worker_id]});
  my $host    = $results->{workers}[0]{host};

These options are currently available:

=over 2

=item ids

  ids => ['23', '24']

List only workers with these ids.

=back

These fields are currently available:

=over 2

=item host

  host => 'localhost'

Worker host.

=item jobs

  jobs => ['10023', '10024', '10025', '10029']

Ids of jobs the worker is currently processing.

=item notified

  notified => 784111777

Epoch time worker sent the last heartbeat.

=item pid

  pid => 12345

Process id of worker.

=item started

  started => 784111777

Epoch time worker was started.

=item status

  status => {queues => ['default', 'important']}

Hash reference with whatever status information the worker would like to share.

=back

=head2 lock

  my $bool = $backend->lock('foo', 3600);
  my $bool = $backend->lock('foo', 3600, {limit => 20});

Try to acquire a named lock that will expire automatically after the given
amount of time in seconds.

These options are currently available:

=over 2

=item limit

  limit => 20

Number of shared locks with the same name that can be active at the same time,
defaults to C<1>.

=back

=head2 note

  my $bool = $backend->note($job_id, foo => 'bar');

Change a metadata field for a job.

=head2 receive

  my $commands = $backend->receive($worker_id);

Receive remote control commands for worker.

=head2 register_worker

  my $worker_id = $backend->register_worker;
  my $worker_id = $backend->register_worker($worker_id);
  my $worker_id = $backend->register_worker(
    $worker_id, {status => {queues => ['default', 'important']}});

Register worker or send heartbeat to show that this worker is still alive.

These options are currently available:

=over 2

=item status

  status => {queues => ['default', 'important']}

Hash reference with whatever status information the worker would like to share.

=back

=head2 remove_job

  my $bool = $backend->remove_job($job_id);

Remove C<failed>, C<finished> or C<inactive> job from queue.

=head2 repair

  $backend->repair;

Repair worker registry and job queue if necessary.

=head2 reset

  $backend->reset;

Reset job queue.

=head2 retry_job

  my $bool = $backend->retry_job($job_id, $retries);
  my $bool = $backend->retry_job($job_id, $retries, {delay => 10});

Transition job back to C<inactive> state, already C<inactive> jobs may also be
retried to change options.

These options are currently available:

=over 2

=item attempts

  attempts => 25

Number of times performing this job will be attempted.

=item delay

  delay => 10

Delay job for this many seconds (from now).

=item priority

  priority => 5

Job priority.

=item queue

  queue => 'important'

Queue to put job in.

=back

=head2 stats

  my $stats = $backend->stats;

Get statistics for jobs and workers.

These fields are currently available:

=over 2

=item active_jobs

  active_jobs => 100

Number of jobs in C<active> state.

=item active_workers

  active_workers => 100

Number of workers that are currently processing a job.

=item delayed_jobs

  delayed_jobs => 100

Number of jobs in C<inactive> state that are scheduled to run at specific time
in the future. Note that this field is EXPERIMENTAL and might change without
warning!

=item enqueued_jobs

  enqueued_jobs => 100000

Rough estimate of how many jobs have ever been enqueued. Note that this field is
EXPERIMENTAL and might change without warning!

=item failed_jobs

  failed_jobs => 100

Number of jobs in C<failed> state.

=item finished_jobs

  finished_jobs => 100

Number of jobs in C<finished> state.

=item inactive_jobs

  inactive_jobs => 100

Number of jobs in C<inactive> state.

=item inactive_workers

  inactive_workers => 100

Number of workers that are currently not processing a job.

=item uptime

  uptime => 1000

Uptime in seconds.

=back

=head2 unlock

  my $bool = $backend->unlock('foo');

Release a named lock.

=head2 unregister_worker

  $backend->unregister_worker($worker_id);

Unregister worker.

=head1 BUGS

Report any issues on the public bugtracker.

=head1 AUTHOR

Dan Book <dbook@cpan.org>

=head1 COPYRIGHT AND LICENSE

This software is Copyright (c) 2017 by Dan Book.

This is free software, licensed under:

  The Artistic License 2.0 (GPL Compatible)

=head1 SEE ALSO

L<Minion>, L<Mojo::Redis2>
