package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
)

func main() {
	// Configura as opções de consumo
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// Cria um consumidor do Kafka
	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalln("Falha ao iniciar o consumidor:", err)
	}

	// Cria um canal de saída
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// Cria um canal de mensagens
	messages := make(chan *sarama.ConsumerMessage, 256)

	// Configura a função de tratamento de erros
	go func() {
		for err := range consumer.Errors() {
			log.Println("Error:", err)
		}
	}()

	// Configura a função de tratamento de sinais de interrupção
	go func() {
		<-signals
		fmt.Println("Sinal de interrupção recebido. Desligando...")
		consumer.Close()
		close(messages)
	}()

	// Cria um consumidor para o tópico de transações do banco
	partitionConsumer, err := consumer.ConsumePartition("bank-transactions", 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatalln("Falha ao iniciar o consumidor para partição:", err)
	}

	// Inicia o loop de consumo de mensagens
	go func() {
		for message := range partitionConsumer.Messages() {
			// Processa a mensagem recebida
			fmt.Println("Mensagem recebida:", string(message.Value))

			// Aqui você pode colocar a lógica de processamento da mensagem recebida
			// Por exemplo, atualizar o saldo da conta do cliente com base na transação recebida
		}
	}()

	// Espera até que o canal de sinais seja fechado
	<-signals
}
