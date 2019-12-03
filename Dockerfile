FROM rabbitmq:3.8.1-management-alpine

RUN rabbitmq-plugins enable rabbitmq_management
RUN rabbitmq-plugins enable rabbitmq_management_agent
RUN rabbitmq-plugins enable rabbitmq_mqtt
RUN rabbitmq-plugins enable rabbitmq_stomp
RUN rabbitmq-plugins enable rabbitmq_federation
RUN rabbitmq-plugins enable rabbitmq_federation_management
RUN rabbitmq-plugins enable rabbitmq_shovel
RUN rabbitmq-plugins enable rabbitmq_shovel_management

ADD config/ /etc/rabbitmq/
ADD definitions.json /etc/rabbitmq/definitions.json
