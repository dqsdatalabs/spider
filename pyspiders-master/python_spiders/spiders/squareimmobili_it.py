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
    name = 'squareimmobili_it'
    execution_type='testing'
    country='italy'
    locale='it' 
    external_source = "Squareimmobili_PySpider_italy"

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.squareimmobili.it/r/annunci/affitto-appartamento-.html?Codice=&Tipologia%5B%5D=1&Motivazione%5B%5D=2&Comune=0&Prezzo_da=&Prezzo_a=&cf=yes",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.squareimmobili.it/r/annunci/affitto-casa-indipendente-.html?Codice=&Tipologia%5B%5D=36&Motivazione%5B%5D=2&Comune=0&Prezzo_da=&Prezzo_a=&cf=yes",
                    "https://www.squareimmobili.it/r/annunci/affitto-attico-.html?Codice=&Tipologia%5B%5D=37&Motivazione%5B%5D=2&Comune=0&Prezzo_da=&Prezzo_a=&cf=yes"
                    "https://www.squareimmobili.it/r/annunci/affitto-casale-.html?Codice=&Tipologia%5B%5D=140&Motivazione%5B%5D=2&Comune=0&Prezzo_da=&Prezzo_a=&cf=yes",
                    "https://www.squareimmobili.it/r/annunci/affitto-cascina-.html?Codice=&Tipologia%5B%5D=46&Motivazione%5B%5D=2&Comune=0&Prezzo_da=&Prezzo_a=&cf=yes"
                    "https://www.squareimmobili.it/r/annunci/affitto-castello-.html?Codice=&Tipologia%5B%5D=315&Motivazione%5B%5D=2&Comune=0&Prezzo_da=&Prezzo_a=&cf=yes",
                    "https://www.squareimmobili.it/r/annunci/affitto-mansarda-.html?Codice=&Tipologia%5B%5D=8&Motivazione%5B%5D=2&Comune=0&Prezzo_da=&Prezzo_a=&cf=yes",
                    "https://www.squareimmobili.it/r/annunci/affitto-villa-.html?Codice=&Tipologia%5B%5D=9&Motivazione%5B%5D=2&Comune=0&Prezzo_da=&Prezzo_a=&cf=yes",
                    "https://www.squareimmobili.it/r/annunci/affitto-villa-a-schiera-.html?Codice=&Tipologia%5B%5D=43&Motivazione%5B%5D=2&Comune=0&Prezzo_da=&Prezzo_a=&cf=yes",
                    "https://www.squareimmobili.it/r/annunci/affitto-villa-bifamiliare-.html?Codice=&Tipologia%5B%5D=51&Motivazione%5B%5D=2&Comune=0&Prezzo_da=&Prezzo_a=&cf=yes",
                    "https://www.squareimmobili.it/r/annunci/affitto-villa-unifamiliare-.html?Codice=&Tipologia%5B%5D=48&Motivazione%5B%5D=2&Comune=0&Prezzo_da=&Prezzo_a=&cf=yes"
                ],
                "property_type": "house"
            },
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
        
        for item in response.xpath("//section/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
        
        next_page = response.xpath("//a[@class='next']/@href").get()
        if next_page:
            print(next_page)
            yield Request(response.urljoin(next_page), callback=self.parse, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        external_id = response.xpath("//div[contains(@class,'codice')]//span//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)

        title = response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title", title)

        address = response.xpath("//div[contains(@class,'dove_schimmo')]//text()").get()
        if address:
            item_loader.add_value("address", address)

        city = response.xpath("//div[contains(@class,'dove_schimmo')]//text()").get()
        if city:
            item_loader.add_value("city", city.split("-")[0])

        description = response.xpath("//div[contains(@class,'testo')]//p//text()").getall()
        if description:
            item_loader.add_value("description", description)

        bathroom_count = response.xpath("//span[contains(text(),'Bagni')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split()[0])

        room_count = response.xpath("//span[contains(text(),'Camere')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split()[0])

        square_meters = response.xpath("//span[contains(text(),'mq')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split(" mq"))

        energy_label = response.xpath("//div[contains(@class,'classe_energ')]//div[contains(@class,'new_c')][1]//text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)

        rent = response.xpath("//div[contains(@class,'details')]//div[contains(@class,'prezzo')][1]//text()").get()
        if rent:
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR") 


        utilities = response.xpath("//strong[contains(text(),'condominio')]/following-sibling::text()").get()
        if utilities:
            utilities = utilities.split()[-1]
            item_loader.add_value("utilities", utilities)        

        latitude_longitude = response.xpath(
            "//script[contains(.,'lgt')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split(
                'lat = "')[1].split('";')[0]
            longitude = latitude_longitude.split(
                'lgt = "')[1].split('";')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        images = [response.urljoin(x) for x in response.xpath("//div[@class='swiper-wrapper']//a[contains(@class,'swipebox')]//@href").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "Square Servizi Immobiliari")
        item_loader.add_value("landlord_phone", "0118123197")
        item_loader.add_value("landlord_email", "info@squareimmobili.it")
        yield item_loader.load_item()