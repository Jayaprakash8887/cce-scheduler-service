package org.openphc.cce.scheduler.domain.repository;

import org.openphc.cce.scheduler.domain.model.SchedulerLease;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface SchedulerLeaseRepository extends JpaRepository<SchedulerLease, UUID> {

    Optional<SchedulerLease> findByPartitionIndex(int partitionIndex);
}
