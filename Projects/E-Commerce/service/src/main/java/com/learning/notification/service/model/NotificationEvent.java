package com.learning.notification.service.model;

import java.time.LocalDateTime;

import org.hibernate.annotations.CreationTimestamp;


import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Entity
@Table(name = "notification_events")
public class NotificationEvent {
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(name = "id",updatable = false,nullable = false)
	private long id;
	
	@Column(name = "event_id",nullable = false,length = 100,unique = true)
	private String eventId;
	
	@Column(name="order_id",nullable = false)
	private long orderId;
	
	@Column(name="status",length = 20)
	private String status;
	
	@CreationTimestamp
	@Column(name = "created_at",updatable = false)
	private LocalDateTime createdAt;
	
	@CreationTimestamp
	@Column(name = "updated_at")
	private LocalDateTime updatedAt;

}
