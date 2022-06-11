package com.example.consumerfacturation.Repositories;

import com.example.consumerfacturation.entities.Facturation;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.rest.core.annotation.RepositoryRestResource;

@RepositoryRestResource
public interface facturationRepository extends JpaRepository<Facturation, Long> {

}
