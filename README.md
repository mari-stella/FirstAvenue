# First Avenue

# 1번가 쇼핑몰

- 체크포인트 : https://workflowy.com/s/assessment-check-po/T5YrzcMewfo4J6LW

# Table of contents

- [1번가 ](#---)
  - [서비스 시나리오](#서비스-시나리오)
  - [분석/설계](#분석설계)
    - [Event Storming 결과](#Event-Storming-결과)
    - [헥사고날 아키텍처 다이어그램 도출](#헥사고날-아키텍처-다이어그램-도출)
  - [구현:](#구현:)
    - [DDD 의 적용](#DDD-의-적용)
    - [기능적 요구사항 검증](#기능적-요구사항-검증)
    - [비기능적 요구사항 검증](#비기능적-요구사항-검증)
    - [Saga](#saga)
    - [CQRS](#cqrs)
    - [Correlation](#correlation)
    - [GateWay](#gateway)
    - [Polyglot](#polyglot)
    - [동기식 호출(Req/Resp) 패턴](#동기식-호출reqresp-패턴)
    - [비동기식 호출 / 시간적 디커플링 / 장애격리 / 최종 (Eventual) 일관성 테스트](#비동기식-호출--시간적-디커플링--장애격리--최종-eventual-일관성-테스트)
  - [운영](#운영)
    - [Deploy / Pipeline](#deploy--pipeline)
    - [Config Map](#configmap)
    - [Secret](#secret)
    - [Circuit Breaker와 Fallback 처리](#circuit-breaker와-fallback-처리)
    - [오토스케일 아웃](#오토스케일-아웃)
    - [Zero-downtime deploy (Readiness Probe) 무정지 재배포](#zero-downtime-deploy-readiness-probe-무정지-재배포)
    - [Self-healing (Liveness Probe))](#self-healing-liveness-probe)

# 서비스 시나리오

## 기능적 요구사항
1. 고객이 상품 주문을 한다.
2. 완료된 주문은 배달이 시작된다.
4. 고객이 주문을 취소할 수 있다.
6. 주문이 취소되면 배달이 취소된다.
7. 관리자가 신규상품을 등록한다.
8. 관리자가 상품 재고를 추가한다.
9. 고객은 회원가입을 한다.
10. 고객은 주문내역을 조회한다.
11. 신규상품이 등록되면 고객에게 알림을 준다.

## 비기능적 요구사항
1. 트랜잭션
    1. 주문 시 재고가 부족할 경우 주문이 되지 않는다. (Sync 호출)
1. 장애격리
    1. 고객/배달 관리 기능이 수행되지 않더라도 주문은 365일 24시간 받을 수 있어야 한다  Async (event-driven), Eventual Consistency
    2. 재고시스템이 과중되면 사용자를 잠시동안 받지 않고 재접속하도록 유도한다  Circuit breaker, fallback


# 분석/설계


## Event Storming 결과
* MSAEz 로 모델링한 이벤트스토밍 결과:


### 이벤트 도출
![image](https://user-images.githubusercontent.com/84316082/123109445-35df7b00-d476-11eb-9e51-9049d2cdb6fe.png)

### 부적격 이벤트 탈락
![image](https://user-images.githubusercontent.com/84316082/123109510-4263d380-d476-11eb-907c-eda383047a3b.png)


### 완성된 1차 모형
![image](https://user-images.githubusercontent.com/84316082/123119997-f23d3f00-d47e-11eb-845b-17d6cf1a6f40.png)

### 1차 완성본에 대한 기능적/비기능적 요구사항을 커버하는지 검증
![image](https://user-images.githubusercontent.com/84316082/123120594-67107900-d47f-11eb-9134-4ffa3d4118f3.png)

    1. 고객이 상품을 주문한다
    2. 주문이 성공하면 배송을 시작한다.
    3. 고객이 주문을 취소할 수 있다.
    4. 주문이 취소되면 배송을 취소한다.

![image](https://user-images.githubusercontent.com/84316082/123120859-a17a1600-d47f-11eb-8c34-26515d75f7cc.png)

    1. 관리자가 신규상품을 등록한다.
    2. 신규 상품이 등록되면 고객에게 알려준다.
    3. 관리자는 상품 재고를 추가한다.

    
### 비기능 요구사항에 대한 검증
![image](https://user-images.githubusercontent.com/84316082/123121636-4b59a280-d480-11eb-9dbd-1d114ad1d562.png)

    1. 신규 주문이 들어올 시 재고를 Sync 호출을 통해 확인하여 결과에 따라 주문 성공 여부가 결정.
    2. 고객/고객센터/배달 각각의 기능은 Async (event-driven) 방식으로 통신, 장애 격리가 가능.
    3. MyPage 를 통해 고객이 주문의 상태를 확인.


## 헥사고날 아키텍처 다이어그램 도출

![image](https://user-images.githubusercontent.com/84316082/123121914-85c33f80-d480-11eb-9272-960ff104dcba.png)
    
    - Chris Richardson, MSA Patterns 참고하여 Inbound adaptor와 Outbound adaptor를 구분함
    - 호출관계에서 PubSub 과 Req/Resp 를 구분함
    - 서브 도메인과 바운디드 컨텍스트의 분리:  각 팀의 KPI 별로 아래와 같이 관심 구현 스토리를 나눠가짐


# 구현:

분석/설계 단계에서 도출된 헥사고날 아키텍처에 따라, 각 BC별로 대변되는 마이크로 서비스들을 스프링부트로 구현하였다. 
구현한 각 서비스를 로컬에서 실행하는 방법은 아래와 같다 (각자의 포트넘버는 8081 ~ 808n 이다)

```
cd gateway
mvn spring-boot:run

cd Product
mvn spring-boot:run 

cd customer
mvn spring-boot:run  

cd CustomerCenter
mvn spring-boot:run  

cd Delivery
mvn spring-boot:run  

cd Order
mvn spring-boot:run  
```

## DDD 의 적용

- 각 서비스내에 도출된 핵심 Aggregate Root 객체를 Entity 로 선언하였다: (예시는 Order 마이크로 서비스). 
- 이때 가능한 현업에서 사용하는 언어 (유비쿼터스 랭귀지)를 그대로 사용하려고 노력했다. 

```
package martdelivery;

import javax.persistence.*;
import org.springframework.beans.BeanUtils;
import java.util.Date;

@Entity
@Table(name="Order_table")
public class Order {

    private Long orderId;
    private Long customerId;
    private Date orderDate;
    private Long productId;
    private Integer qty;
    private Integer price;
    private Integer amt;
    private String address;
    private String status;

    @PrePersist
    public void onPrePersist(){
        // Req/Res Calling
        boolean bResult = false;
        try{
            bResult = OrderApplication.applicationContext.getBean(martdelivery.external.ProductService.class)
                    .checkAndModifyStock(this.productId, this.qty);
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }


        this.amt = qty*price;
        this.orderDate = new Date();

        if(bResult)
        {
            this.status="Ordered";
        }
        else
        {
            this.status="OutOfStocked";
        }
    }


    @PostPersist
    public void onPostPersist(){
        if(this.status.equals("Ordered"))
        {
            Ordered ordered = new Ordered();
            BeanUtils.copyProperties(this, ordered);
            ordered.publishAfterCommit();
            System.out.println("** PUB :: Ordered : orderId="+this.orderId);
        }
        else
        {
            OutOfStocked outOfStocked = new OutOfStocked();
            BeanUtils.copyProperties(this, outOfStocked);
            outOfStocked.publish();
            System.out.println("** PUB :: OutOfStocked : orderId="+this.orderId);
        }
    }

    @PreUpdate
    public void onPreUpdate(){
        if(this.status.equals("Order Cancelled"))
        {
            System.out.println("** PUB :: OrderCancelled : orderId" + this.orderId);
            OrderCancelled orderCancelled = new OrderCancelled();
            BeanUtils.copyProperties(this, orderCancelled);
            orderCancelled.publishAfterCommit();
        }
        else {
            System.out.println("** PUB :: StatusChanged : status changed to " + this.status.toString());
            StatusChanged statusChanged = new StatusChanged();
            BeanUtils.copyProperties(this, statusChanged);
            statusChanged.publishAfterCommit();
        }
    }

    public Long getOrderId() {
        return orderId;
    }
    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public Long getCustomerId() {
        return customerId;
    }
    public void setCustomerId(Long customerId) {
        this.customerId = customerId;
    }

    public Date getOrderDate() {
        return orderDate;
    }
    public void setOrderDate(Date orderDate) {
        this.orderDate = orderDate;
    }

    public Long getProductId() {
        return productId;
    }
    public void setProductId(Long productId) {
        this.productId = productId;
    }

    public Integer getQty() {
        return qty;
    }
    public void setQty(Integer qty) {
        this.qty = qty;
    }

    public Integer getPrice() {
        return price;
    }
    public void setPrice(Integer price) {
        this.price = price;
    }

    public Integer getAmt() {
        return amt;
    }
    public void setAmt(Integer amt) {
        this.amt = amt;
    }

    public String getAddress() {
        return address;
    }
    public void setAddress(String address) {
        this.address = address;
    }

    public String getStatus() {
        return status;
    }
    public void setStatus(String status) {
        this.status = status;
    }
}


```

- Entity Pattern 과 Repository Pattern 을 적용하여 JPA 를 통한 다양한 데이터소스 유형 (MySQL or h2) 에 대한 별도의 처리가 없도록 데이터 접근 어댑터를 자동 생성하기 위해 Spring Data REST 의 RestRepository 를 적용하였다.
- (로컬개발환경에서는 MySQL/H2를, 쿠버네티스에서는 SQLServer/H2를 각각 사용하였다)
```
package martdelivery;

import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.data.rest.core.annotation.RepositoryRestResource;
import java.util.Optional;

@RepositoryRestResource(collectionResourceRel="orders", path="orders")
public interface OrderRepository extends PagingAndSortingRepository<Order, Long>{

    Optional<Order> findByOrderId(Long orderId);

}

```

- 적용 후 REST API 의 테스트
```
# Order 서비스의 주문처리
http POST http://localhost:8088/orders orderId=1 customerId=1 productId=1 qty=1 

# Book 서비스의 재입고
http PATCH http://localhost:8088/products/reStock productId=1  stock=1000

# 주문 상태 확인
http GET http://localhost:8088/myPages

```

## 기능적 요구사항 검증

1. 고객이 상품을 주문한다.

--> 정상적으로 주문됨을 확인하였음 

![image](https://user-images.githubusercontent.com/84316082/123159460-00ec1c00-d4a8-11eb-946a-bc98e9fdcdb7.png)

2. 주문이 성공하면 배송을 시작한다.

--> 정상적으로 배송 시작됨을 확인하였음 

![image](https://user-images.githubusercontent.com/84316082/123161103-f92d7700-d4a9-11eb-8ed0-00661953c6d9.png)


3. 고객이 주문을 취소할 수 있다.

--> 정상적으로 취소됨을 확인하였음 

![image](https://user-images.githubusercontent.com/84316082/123162383-80c7b580-d4ab-11eb-8fe1-e7e955946e4f.png)


4. 주문이 취소되면 배송을 취소한다.

--> 주문과 배송 시스템에서 각각 취소되었음을 확인하였음 

![image](https://user-images.githubusercontent.com/84316082/123162394-86bd9680-d4ab-11eb-929a-075db473059e.png)


5. 관리자가 신규 상품을 등록한다.

--> 정상적으로 등록됨을 확인하였음

![image](https://user-images.githubusercontent.com/84316082/123150399-47884900-d49d-11eb-9883-6dfa3632353d.png)


6. 관리자가 상품 재고를 추가한다.

--> 정상적으로 재고가 늘어남을 확인하였음

![image](https://user-images.githubusercontent.com/84316082/123150634-90400200-d49d-11eb-80ca-5d1febf91222.png)
![image](https://user-images.githubusercontent.com/84316082/123150697-9e8e1e00-d49d-11eb-8ddb-9abde294b894.png)


7. 고객은 회원가입을 한다.

--> 정상적으로 등록됨을 확인하였음

![image](https://user-images.githubusercontent.com/84316082/123151952-fa0cdb80-d49e-11eb-9406-c839f27d1754.png)


8. 신규 상품이 등록되면 고객에게 알려준다.

--> EMAIL이 발송됨을 확인하였음 

![image](https://user-images.githubusercontent.com/84316082/123155686-5c67db00-d4a3-11eb-8009-1dc1ebf69745.png)


## 비기능적 요구사항 검증

1. 트랜잭션

주문 시 재고가 부족할 경우 주문이 되지 않는다. (Sync 호출)

--> 재고보다 많은 양(qty)을 주문하였을 경우 OutOfStock으로 처리한다.

![image](https://user-images.githubusercontent.com/84316082/123158296-91296180-d4a6-11eb-8c64-9c3135245b81.png)
![image](https://user-images.githubusercontent.com/84316082/123158316-99819c80-d4a6-11eb-8c54-19e2c572ec8b.png)



2. 장애격리
고객/고객센터/배달 관리 기능이 수행되지 않더라도 주문은 365일 24시간 받을 수 있어야 한다 Async (event-driven), Eventual Consistency

--> 고객/고객센터/배달 마이크로서비스를 모두 내리고 주문을 생성했을때, 정상적으로 주문됨을 확인함

![image](https://user-images.githubusercontent.com/84316082/123169288-0bacae00-d4b4-11eb-8aad-23bb6d3a506e.png
![image](https://user-images.githubusercontent.com/84316082/123169313-16674300-d4b4-11eb-8f2a-fcda625147a4.png)



3. 재고시스템이 과중되면 사용자를 잠시동안 받지 않고 재접속하도록 유도한다 Circuit breaker, fallback

--> 뒤의 Hystrix를 통한 Circuit Break 구현에서 검증하도록 한다.


## Saga
분석/설계 및 구현을 통해 이벤트를 Publish/Subscribe 하도록 구현하였다.

[Publish]

![image](https://user-images.githubusercontent.com/84316082/123169413-3c8ce300-d4b4-11eb-9058-6953077c7ae1.png)

[Subscribe]

![image](https://user-images.githubusercontent.com/84316082/123169558-66460a00-d4b4-11eb-9cae-6c7a0db7c863.png)


## CQRS
Materialized View 를 구현하여, 타 마이크로서비스의 데이터 원본에 접근없이(Composite 서비스나 조인SQL 등 없이) 도 내 서비스의 화면 구성과 잦은 조회가 가능하게 구현해 두었다.

본 프로젝트에서 View 역할은 CustomerCenter 서비스가 수행한다.

CQRS를 구현하여 주문건에 대한 상태는 Order 마이크로서비스의 접근없이 CustomerCenter의 마이페이지를 통해 조회할 수 있도록 구현하였다.

- 주문(ordered) 실행 후 myPage 화면

![image](https://user-images.githubusercontent.com/84316082/123170923-0e100780-d4b6-11eb-858a-50b1d17058bc.png)

- 주문취소(OrderCancelled) 후 myPage 화면

![image](https://user-images.githubusercontent.com/84316082/123170983-254ef500-d4b6-11eb-852a-cda6066f319d.png)


위와 같이 주문을 하게되면 Order -> Product -> Order -> Delivery 로 주문이 Assigend 되고

주문 취소가 되면 Status가 "Delivery Cancelled"로 Update 되는 것을 볼 수 있다.

## Correlation 
각 이벤트 건(메시지)이 어떤 Policy를 처리할 때 어떤건에 연결된 처리건인지를 구별하기 위한 Correlation-key를 제대로 연결하였는지를 검증하였다.

![image](https://user-images.githubusercontent.com/84316082/123184000-bed5d100-d4cd-11eb-9fba-3d6e11b64208.png)


## GateWay 
API GateWay를 통하여 마이크로 서비스들의 진입점을 통일할 수 있다.
다음과 같이 GateWay를 적용하여 모든 마이크로서비스들은 http://localhost:8088/{context}로 접근할 수 있다.

(gateway) application.yaml
``` 

server:
  port: 8088

---

spring:
  profiles: default
  cloud:
    gateway:
      routes:
        - id: Product
          uri: http://localhost:8081
          predicates:
            - Path=/products/** 
        - id: Order
          uri: http://localhost:8082
          predicates:
            - Path=/orders/** 
        - id: Delivery
          uri: http://localhost:8083
          predicates:
            - Path=/deliveries/** 
        - id: CustomerCenter
          uri: http://localhost:8084
          predicates:
            - Path= /marketing/**,/myPages/**
        - id: Customer
          uri: http://localhost:8085
          predicates:
            - Path=/customers/** 
      globalcors:
        corsConfigurations:
          '[/**]':
            allowedOrigins:
              - "*"
            allowedMethods:
              - "*"
            allowedHeaders:
              - "*"
            allowCredentials: true


---

spring:
  profiles: docker
  cloud:
    gateway:
      routes:
        - id: Product
          uri: http://Product:8080
          predicates:
            - Path=/products/** 
        - id: Order
          uri: http://Order:8080
          predicates:
            - Path=/orders/** 
        - id: Delivery
          uri: http://Delivery:8080
          predicates:
            - Path=/deliveries/** 
        - id: CustomerCenter
          uri: http://CustomerCenter:8080
          predicates:
            - Path= /marketing/**,/myPages/**
        - id: Customer
          uri: http://Customer:8080
          predicates:
            - Path=/customers/** 
      globalcors:
        corsConfigurations:
          '[/**]':
            allowedOrigins:
              - "*"
            allowedMethods:
              - "*"
            allowedHeaders:
              - "*"
            allowCredentials: true

server:
  port: 8080
```


## Polyglot

각 마이크로서비스의 다양한 요구사항에 능동적으로 대처하고자 최적의 구현언어 및 DBMS를 선택할 수 있다.
OnlineBookStore에서는 다음과 같이 2가지 DBMS를 적용하였다.
- MySQL(쿠버네티스에서는 SQLServer) : Product, CustomerCenter, Customer, Delivery
- H2    : Order

```
# (Product, CustomerCenter, Customer, Delivery) application.yml

spring:
  profiles: default
  driver-class-name: com.mysql.cj.jdbc.Driver
  url: jdbc:mysql://localhost:3306/productDB?useSSL=false&characterEncoding=UTF-8&serverTimezone=UTC&allowPublicKeyRetrieval=true
  username: ******
  password: ****

spring:
  profiles: docker
  datasource:
    driver-class-name: com.microsoft.sqlserver.jdbc.SQLServerDriver
    url: jdbc:sqlserver://skccteam2.database.windows.net:1433;database=bookstore;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;
    username: ${SQLSERVER_USERNAME}
    password: ${SQLSERVER_PASSWORD}
...

# (Order) application.yml

spring:
  profiles: default
  datasource:
    driver-class-name: org.h2.Driver
    url: jdbc:h2:file:/data/orderdb
    username: *****
    password: 
```


## 동기식 호출(Req/Resp) 패턴

분석단계에서의 조건 중 하나로 주문(Order)->책 재고 확인(Book) 간의 호출은 동기식 일관성을 유지하는 트랜잭션으로 처리하기로 하였다. 
호출 프로토콜은 RestController를 FeignClient 를 이용하여 호출하도록 한다. 

- 재고 확인 서비스를 호출하기 위하여 Stub과 (FeignClient) 를 이용하여 Service 대행 인터페이스 (Proxy) 를 구현 

```
# (Order) ProductService.java

package martdelivery.external;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.*;

import java.util.Date;

@FeignClient(name="Product", url="${api.url.product}", fallbackFactory = ProductServiceFallbackFactory.class)
public interface ProductService {
    @RequestMapping(method= RequestMethod.GET, path="/products/checkAndModifyStock")
    public boolean checkAndModifyStock(@RequestParam("productId") Long productId,
                                    @RequestParam("qty") int qty);

}
```

- 주문을 받은 직후 재고(Product) 확인을 요청하도록 처리
```
# ProductController.java

package martdelivery;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.List;
import java.util.Optional;

@RestController
 public class ProductController {
     @Autowired  ProductRepository productRepository;

     @RequestMapping(value = "/products/checkAndModifyStock",
          method = RequestMethod.GET,
          produces = "application/json;charset=UTF-8")
     public boolean chkAndModifyStock(@RequestParam("productId") Long productId,
                                      @RequestParam("qty")  int qty)
             throws Exception {

         System.out.println("##### /products/checkAndModifyStock  called #####");
         boolean status = false;
         Optional<Product> productOptional = productRepository.findByProductId(productId);
         if (productOptional.isPresent()) {
             Product product = productOptional.get();
             // 현 재고보다 주문수량이 적거나 같은경우에만 true 회신
             if( product.getQty() >= qty){
                 status = true;
                 product.setStockBeforeUpdate(product.getQty());
                 product.setQty(product.getQty() - qty); // 주문수량만큼 재고 감소
                 productRepository.save(product);
             }
         }
         return status;
     }
     
```

- 동기식 호출에서는 호출 시간에 따른 타임 커플링이 발생하며, 상품 관리 시스템이 장애가 나면 주문도 못받는다는 것을 확인:


```
# 상품 관리 (Product) 서비스를 잠시 내려놓음 (ctrl+c)

#주문처리
http POST http://localhost:8088/orders  customerId=1 productId=2 qty=1   #Fail
http POST http://localhost:8088/orders  customerId=3 productId=1 qty=1   #Fail

#재고 관리 서비스 재기동
cd Product
mvn spring-boot:run

#주문처리

http POST http://localhost:8088/orders  customerId=1 productId=2 qty=1   #Success
http POST http://localhost:8088/orders  customerId=3 productId=1 qty=1   #Success 
```
추후 운영단계에서는 Circuit Breaker를 이용하여 상품 관리 시스템에 장애가 발생하여도 주문 접수는 가능하도록 개선할 예정이다.


## 비동기식 호출 / 시간적 디커플링 / 장애격리 / 최종 (Eventual) 일관성 테스트

주문이 이루어진 후에 배송 시스템으로 이를 알려주는 행위는 동기식이 아니라 비 동기식으로 처리하여 배송 시스템의 처리를 위하여 주문이 블로킹 되지 않도록 처리한다.
 
- 이를 위하여 주문이력에 기록을 남긴 후에 곧바로 주문이 완료되었다는 도메인 이벤트를 카프카로 송출한다(Publish)
 
```
package martdelivery;

import javax.persistence.*;
import org.springframework.beans.BeanUtils;
import java.util.Date;

@Entity
@Table(name="Order_table")
public class Order {

 ...

    @PostPersist
    public void onPostPersist(){
        if(this.status.equals("Ordered"))
        {
            Ordered ordered = new Ordered();
            BeanUtils.copyProperties(this, ordered);
            ordered.publishAfterCommit();
            System.out.println("** PUB :: Ordered : orderId="+this.orderId);
        }
        else
        {
            OutOfStocked outOfStocked = new OutOfStocked();
            BeanUtils.copyProperties(this, outOfStocked);
            outOfStocked.publish();
            System.out.println("** PUB :: OutOfStocked : orderId="+this.orderId);
        }
    }
 
 ...

}
```
- 배송관리 서비스에서는 주문 완료 이벤트에 대해서 이를 수신하여 자신의 정책을 처리하도록 PolicyHandler 를 구현한다:

```
package martdelivery;

...

@Service
public class PolicyHandler{

    @StreamListener(KafkaProcessor.INPUT)
    public void wheneverOrdered_Delivery(@Payload Ordered ordered){
        if(!ordered.validate()) return;
        System.out.println("\n\n##### listener Delivery : " + ordered.toJson() + "\n\n");
        Delivery delivery = new Delivery();
        
        delivery.setOrderId(ordered.getOrderId());
        delivery.setProductId(ordered.getProductId());
        delivery.setAddress(ordered.getAddress());
        delivery.setQty(ordered.getQty());
        delivery.setStatus("Order-Delivery");

        deliveryRepository.save(delivery);
    }

}

```

배송 시스템은 주문/재고관리와 완전히 분리되어있으며, 이벤트 수신에 따라 처리되기 때문에, 배송 시스템이 유지보수로 인해 잠시 내려간 상태라도 주문을 받는데 문제가 없다:
```
# 배송관리 서비스 (Delivery) 를 잠시 내려놓음 (ctrl+c)

#주문처리
http POST http://localhost:8088/orders  customerId=2 productId=1 qty=2   #Success
http POST http://localhost:8088/orders  customerId=3 productId=1 qty=1   #Success 

#주문상태 확인
http http://localhost:8088/orders     # 주문상태 안바뀜 확인

#배송 서비스 기동
cd Delivery
mvn spring-boot:run

#주문상태 확인
http http://localhost:8088/orders     # 모든 주문의 상태가 "Delivery Started"로 확인
```


# 운영

## Deploy / Pipeline

- git에서 소스 가져오기
```
git clone https://github.com/mari-stella/FirstAvenue.git
```
- Build 하기
```
cd /product
mvn package

cd ../customer
mvn package

cd ../customercenter
mvn package

cd ../order
mvn package

cd ../delivery
mvn package

cd ../gateway
mvn package

```

- Docker Image build/Push/
```

cd ../gateway
docker build -t skccteam2acr.azurecr.io/gateway:latest .
docker push skccteam2acr.azurecr.io/gateway:latest

cd ../product
docker build -t skccteam2acr.azurecr.io/product:latest .
docker push skccteam2acr.azurecr.io/product:latest

cd ../customer
docker build -t skccteam2acr.azurecr.io/customer:latest .
docker push skccteam2acr.azurecr.io/customer:latest

cd ../customercenter
docker build -t skccteam2acr.azurecr.io/customercenter:latest .
docker push skccteam2acr.azurecr.io/customercenter:latest

cd ../order
docker build -t skccteam2acr.azurecr.io/order:latest .
docker push skccteam2acr.azurecr.io/order:latest

cd ../delivery
docker build -t skccteam2acr.azurecr.io/delivery:latest .
docker push skccteam2acr.azurecr.io/delivery:latest


```

- yml파일 이용한 deploy
```
kubectl apply -f deployment.yml

- martdelivery/Order/kubernetes/deployment.yml 파일 
apiVersion: apps/v1
kind: Deployment
metadata:
  name: order
  namespace: martdelivery
  labels:
    app: order
spec:
  replicas: 1
  selector:
    matchLabels:
      app: order
  template:
    metadata:
      labels:
        app: order
    spec:
      containers:
        - name: order
          image: skccteam2acr.azurecr.io/order:latest
          ports:
            - containerPort: 8080
          readinessProbe:
            httpGet:
              path: '/actuator/health'
              port: 8080
            initialDelaySeconds: 10
            timeoutSeconds: 2
            periodSeconds: 5
            failureThreshold: 10
          livenessProbe:
            httpGet:
              path: '/actuator/health'
              port: 8080
            initialDelaySeconds: 120
            timeoutSeconds: 2
            periodSeconds: 5
            failureThreshold: 5
          env:
            - name: configmap
              valueFrom:
                configMapKeyRef:
                  name: resturl
                  key: url
          resources:
            requests:
              cpu: 300m
              # memory: 256Mi
            limits:
              cpu: 500m
              # memory: 256Mi
```	  

- deploy 완료

![image](https://user-images.githubusercontent.com/20077391/121022073-fc9fdd80-c7dc-11eb-9f50-962556056728.png)


## ConfigMap 
- 시스템별로 변경 가능성이 있는 설정들을 ConfigMap을 사용하여 관리
- 본 시스템에서는 주문에서 상품 서비스 호출 시 "호출 주소"를 ConfigMap 처리하기로 결정

- Java 소스에 "호출 주소"를 변수(api.url.product) 처리(/Order/src/main/java/martdelivery/external/ProductService.java) 

![image](https://user-images.githubusercontent.com/84316082/123186441-19bdf700-d4d3-11eb-96ec-ce2082afb19d.png)


- application.yml 파일에서 api.url.product ConfigMap과 연결

![image](https://user-images.githubusercontent.com/84316082/123186601-74efe980-d4d3-11eb-8bbe-a4001dbda93a.png)


- ConfigMap 생성

```
kubectl create configmap resturl --from-literal=url=http://Product:8080
```

- Deployment.yml 에 ConfigMap 적용

![image](https://user-images.githubusercontent.com/20077391/120965103-58e40c80-c79f-11eb-8abd-d3a98048166e.png)


## Secret 
- DBMS 연결에 필요한 username 및 password는 민감한 정보이므로 Secret 처리하였다.

![image](https://user-images.githubusercontent.com/84316082/123190991-7f15e600-d4db-11eb-84e7-bd56aa029fd9.png)

- deployment.yml에서 env로 설정하였다.

![image](https://user-images.githubusercontent.com/84316082/123191183-d1570700-d4db-11eb-8b03-03ad81956a95.png)

- 쿠버네티스에서는 다음과 같이 Secret object를 생성하였다.

![image](https://user-images.githubusercontent.com/84316082/123191851-d23c6880-d4dc-11eb-81ad-80c57c07fcb6.png)
![image](https://user-images.githubusercontent.com/84316082/123230778-899fa200-d512-11eb-8aa2-e82c1dd834d5.png)

## Circuit Breaker와 Fallback 처리

* Spring FeignClient + Hystrix를 사용하여 구현함

시나리오는 주문(Order)-->상품(Product) 확인 시 주문 요청에 대한 재고확인이 3초를 넘어설 경우 Circuit Breaker 를 통하여 장애격리.

- Hystrix 를 설정:  FeignClient 요청처리에서 처리시간이 3초가 넘어서면 CB가 동작하도록 (요청을 빠르게 실패처리, 차단) 설정
                    추가로, 테스트를 위해 1번만 timeout이 발생해도 CB가 발생하도록 설정
```
# (Order) application.yml
```
![image](https://user-images.githubusercontent.com/84316082/123192320-9eae0e00-d4dd-11eb-9e02-04f7adc2938e.png)


- 호출 서비스(주문)에서는 재고API 호출에서 문제 발생 시 주문건을 OutOfStock 처리하도록 FallBack 구현
```
# (Order) ProductService.java 
```
![image](https://user-images.githubusercontent.com/84316082/123192807-960a0780-d4de-11eb-8072-0dbf886cee7c.png)


- 피호출 서비스(상품:Product)에서 테스트를 위해 productId 2인 주문건에 대해 sleep 처리
```
# (Product) ProductController.java 
```
![image](https://user-images.githubusercontent.com/84316082/123201232-36672880-d4ed-11eb-95b8-ff019ab397bf.png)


* 서킷 브레이커 동작 확인:

bookId가 1번 인 경우 정상적으로 주문 처리 완료
```
# http POST http://52.231.54.4:8080/orders  customerId=1 productId=1 qty=1
```
![image](https://user-images.githubusercontent.com/84316082/123257862-aa292580-d52d-11eb-851e-22659a225f60.png)

bookId가 2번 인 경우 CB에 의한 timeout 발생 확인 (Order건은 OutOfStocked 처리됨)
```
# http POST http://52.231.54.4:8080/orders  customerId=1 productId=2 qty=1
```
![image](https://user-images.githubusercontent.com/84316082/123257734-8534b280-d52d-11eb-939f-aed39597c35c.png)

time 아웃이 연달아 2번 발생한 경우 CB가 OPEN되어 Product 호출이 아예 차단된 것을 확인 (테스트를 위해 circuitBreaker.requestVolumeThreshold=1 로 설정)

```
# http POST http://52.231.54.4:8080/orders  customerId=1 productId=3 qty=1
```
![image](https://user-images.githubusercontent.com/84316082/123258050-e52b5900-d52d-11eb-84f6-91f16f22e03f.png)


일정시간 뒤에는 다시 주문이 정상적으로 수행되는 것을 알 수 있다.
```
# http POST http://52.231.54.4:8080/orders  customerId=1 productId=3 qty=1
```
![image](https://user-images.githubusercontent.com/84316082/123257336-fd4ea880-d52c-11eb-9607-9268918828c5.png)


- 운영시스템은 죽지 않고 지속적으로 CB 에 의하여 적절히 회로가 열림과 닫힘이 벌어지면서 Thread 자원 등을 보호하고 있음을 보여줌.



### 오토스케일 아웃
주문 서비스가 몰릴 경우를 대비하여 자동화된 확장 기능을 적용하였다.

- 주문서비스에 대한 replica 를 동적으로 늘려주도록 HPA 를 설정한다. 설정은 테스트를 위해 CPU 사용량이 50프로를 넘어서면 replica 를 3개까지 늘려준다:
```
hpa.yml
```
![image](https://user-images.githubusercontent.com/84316082/123257176-c8daec80-d52c-11eb-9ad8-7600df025ade.png)


- deployment.yml에 resource 관련 설정을 추가해 준다.
```
deployment.yml
```
![image](https://user-images.githubusercontent.com/20077391/121101100-25a08c80-c836-11eb-81f1-a7df0f0dcaeb.png)


- 100명이 60초 동안 주문을 넣어준다.
```
siege  -c100 -t60S  -v --content-type "application/json" 'http://10.0.110.238:8080/orders POST {"customerId":"1" ,"productId": "1", "qty":1}'
```

- 오토스케일이 어떻게 되고 있는지 모니터링을 걸어둔다:
```
kubectl get deploy -l app=order -w
```

- 어느정도 시간이 흐른 후 스케일 아웃이 벌어지는 것을 확인할 수 있다.

![image](https://user-images.githubusercontent.com/84316082/123238006-1cdbd600-d519-11eb-8c9c-2c39c7a17593.png)


- siege 의 로그를 보면 오토스케일 확장이 일어나며 주문을 100% 처리완료한 것을 알 수 있었다.
![image](https://user-images.githubusercontent.com/84316082/123257002-916c4000-d52c-11eb-8620-33fad25f2620.png)





## Zero-downtime deploy (Readiness Probe) 무정지 재배포

* Zero-downtime deploy를 위해 readiness Probe를 설정함

![image](https://user-images.githubusercontent.com/84316082/123239974-da1afd80-d51a-11eb-9a9e-3a9b5dd8f9ad.png)


* Zero-downtime deploy 확인을 위해 seige 로 1명이 지속적인 고객등록 작업을 수행함
```
siege -c1 -t120S -r100 --content-type "application/json" 'http://10.0.234.253:8080/customers POST {"loginId": "testasb3","password":"112233", "email":"testasb3@firstavn.com"}'
```

- 먼저 customer 이미지가 v1.0 임을 확인
![image](https://user-images.githubusercontent.com/84316082/123256732-42260f80-d52c-11eb-9f48-ab52c57b348c.png)


- 새 버전으로 배포(이미지를 v2.0으로 변경)
```
kubectl set image deployment customer customer=user09acr.azurecr.io/customer:v2.0
```

- customer 이미지가 변경되는 과정 (POD 상태변화)
![image](https://user-images.githubusercontent.com/84316082/123241352-06834980-d51c-11eb-975e-ed13913bc751.png)



- customer 이미지가 v2.0으로 변경되었임을 확인
![image](https://user-images.githubusercontent.com/84316082/123256644-29b5f500-d52c-11eb-827f-fb5e7b432678.png)


- seige 의 화면으로 넘어가서 Availability가 100% 인지 확인 (무정지 배포 성공)
![image](https://user-images.githubusercontent.com/84316082/123256561-1014ad80-d52c-11eb-85cc-3859703f647b.png)


# Self-healing (Liveness Probe)

- Self-healing 확인을 위한 Liveness Probe 옵션 변경 (Port 변경)

onlinebookstore/delivery/kubernetes/deployment.yml

![image](https://user-images.githubusercontent.com/84316082/123256488-fd01dd80-d52b-11eb-9248-b1b2ca5d639f.png)


- Delivery pod에 Liveness Probe 옵션 적용 확인

![image](https://user-images.githubusercontent.com/84316082/123254979-47825a80-d52a-11eb-8bea-17038be3e535.png)
![image](https://user-images.githubusercontent.com/84316082/123256424-e8bde080-d52b-11eb-95f7-e73b361ac1a3.png)

- Liveness 확인 실패에 따른 retry발생 확인

![image](https://user-images.githubusercontent.com/84316082/123255713-2110ef00-d52b-11eb-9c6e-3eb243872889.png)


이상으로 12가지 체크포인트가 구현 및 검증 완료되었음 확인하였다.

# 끗~
