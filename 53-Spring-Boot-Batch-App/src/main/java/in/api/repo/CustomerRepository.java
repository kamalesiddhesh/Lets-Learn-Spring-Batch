package in.api.repo;

import java.io.Serializable;

import org.springframework.data.jpa.repository.JpaRepository;

import in.api.entity.Customer;

public interface CustomerRepository extends JpaRepository<Customer,Serializable>{

}
