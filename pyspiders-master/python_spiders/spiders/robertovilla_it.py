# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'robertovilla_it'
    execution_type='testing'
    country='italy'
    locale='it'
    external_source = 'Robertovilla_PySpider_italy'

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "http://robertovilla.it/elenco/in_Affitto/Residenziale/Appartamento/tutte_le_province/tutti_i_comuni/tutte_le_zone/?idt=13&ordinamento1=5&ordinamento2=decrescente",
                ],
                "property_type" : "apartment"
            },
            {
                "url": [
                    "http://robertovilla.it/elenco/in_Affitto/Residenziale/Villa/tutte_le_province/tutti_i_comuni/tutte_le_zone/?idt=17&ordinamento1=5&ordinamento2=decrescente",
                    "http://robertovilla.it/elenco/in_Affitto/Residenziale/Villa_a_schiera/tutte_le_province/tutti_i_comuni/tutte_le_zone/?idt=19&ordinamento1=5&ordinamento2=decrescente"          
                ],
                "property_type" : "house"
            }
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='descrizione-esito']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        title = response.xpath("//meta[contains(@name,'description')]/@content").get()
        if title:
            item_loader.add_value("title", title)

        external_id = response.xpath("//li[@class='caratteristiche-li'][contains(.,'Riferimento')]/strong/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)

        rent = response.xpath("//li[@class='caratteristiche-li'][contains(.,'Prezzo')]/strong/text()").get()
        if rent:
            item_loader.add_value("rent", rent.split(',')[0].replace(".","").strip())
        item_loader.add_value("currency","EUR")

        square_meters = response.xpath("//li[@class='caratteristiche-li'][contains(.,'Superficie mq')]/strong/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters)

        room_count = response.xpath("//li[@class='caratteristiche-li'][contains(.,'Locali')]/strong/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//li[@class='caratteristiche-li'][contains(.,'Camere')]/strong/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//li[@class='caratteristiche-li'][contains(.,'Bagni')]/strong/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        city = response.xpath("//li[@class='caratteristiche-li'][contains(.,'Comune')]/strong/text()").get()
        if city:
            item_loader.add_value("city", city)

        address = response.xpath("//div[@class='entry-dettaglio']/h4/text()[contains(.,'Localizzazione')]").get()
        if address:
            item_loader.add_value("address", address.split('-')[-1].split('via')[-1].strip())

        desc = "".join(response.xpath("//div[@class='entry-dettaglio']/p/text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())

        elevator = response.xpath("//li[@class='caratteristiche-li2']/strong/text()").get()
        if elevator and "ascensore" in elevator.lower():
            item_loader.add_value("elevator", True)
        if elevator and "balcon" in elevator.lower():
            item_loader.add_value("balcony", True)

        energy_label = response.xpath("//li[@class='caratteristiche-li'][contains(.,'energetica')]/strong/text()").get()
        if energy_label:
            energy_label = "".join(energy_label.split(' ')[1:2])
            item_loader.add_value("energy_label", energy_label)

        latlng = response.xpath("//script[@type='text/javascript'][contains(.,'Latitudine')]/text()").get()
        if latlng:
            latitude = latlng.split('Latitudine=')[-1].split('&')[0]
            item_loader.add_value("latitude", latitude)
            longitude = latlng.split('Longitudine=')[-1].split('&')[0]
            item_loader.add_value("longitude", longitude)
        
        images = [x for x in response.xpath("//div[@u='slides']//div//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        item_loader.add_value("landlord_name", "Immobiliare Villa di Lucio Antonini")
        item_loader.add_value("landlord_phone", "02.312626")
        item_loader.add_value("landlord_email", "ufficio@immobiliarevillamilano.it")
        yield item_loader.load_item()