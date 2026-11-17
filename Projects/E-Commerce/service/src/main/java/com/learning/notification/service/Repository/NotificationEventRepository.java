package com.learning.notification.service.Repository;

import org.springframework.stereotype.Repository;
import com.learning.notification.service.model.NotificationEvent;
import org.springframework.data.jpa.repository.JpaRepository;

@Repository
public interface NotificationEventRepository extends JpaRepository<NotificationEvent, Long> {
	boolean existsByEvent_id(String event_id);
}
