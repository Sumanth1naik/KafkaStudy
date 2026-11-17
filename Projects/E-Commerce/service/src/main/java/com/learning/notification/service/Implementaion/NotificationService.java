package com.learning.notification.service.Implementaion;

import org.springframework.stereotype.Service;

import com.learning.notification.service.Repository.NotificationEventRepository;

@Service
public class NotificationService {

	private final NotificationEventRepository notificationEventRepository;
	public NotificationService(NotificationEventRepository notificationEventRepository) {
		this.notificationEventRepository = notificationEventRepository;
	}
	public boolean checkEvent(String eventId) {
		return notificationEventRepository.existsByEvent_id(eventId);
	}
}
