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
    name = 'direzionecasa2017_it'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Direzionecasa2017_Pyspider_italy"
    start_urls = ['http://www.direzionecasa2017.it/index.php']  # LEVEL 1

    def start_requests(self):
        start_urls = [
	        {
                "url": [
                    "http://www.direzionecasa2017.it/index.php"
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
        # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//a[contains(.,'Dettagli')]//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        external_id=response.xpath("//span[contains(.,'Codice riferimento annuncio: ')]//following-sibling::b//text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.replace("\u00a0",""))

        title=response.xpath("//div[@class='mainDescriptionTitle rounded_box_title']//h3//text()").get()
        if title:
            item_loader.add_value("title",title)

        city="".join(response.xpath("//td[contains(.,'Zona')]//following-sibling::td//span//text()").get())
        if city:
            item_loader.add_value("city",city)

        address=response.xpath("//td[contains(.,'Indirizzo')]//following-sibling::td//span//text()").get()
        if address:
            item_loader.add_value("address",address)

        rent=response.xpath("//td[contains(.,'Prezzo')]//following-sibling::td//span//text()").get()
        if rent:
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","EUR")

        square_meters=response.xpath("//td[contains(.,'Mq')]//following-sibling::td//span//text()").get()
        if 'Mq' in square_meters:
            item_loader.add_value("square_meters",square_meters.split("Mq")[1])

        room_count=response.xpath("//td[contains(.,'Camere')]//following-sibling::td//span//text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)

        description=response.xpath("//p[contains(@align,'justify')]/text()").getall()
        if description:
            item_loader.add_value("description",description)

        energy_label=response.xpath("//img[contains(@src,'pannello/classe')]//@src").get()
        if energy_label:
            item_loader.add_value("energy_label",energy_label.split("classe/")[1].split(".jpg"))


        images = [response.urljoin(x)for x in response.xpath("//img[@alt='Clicca per ingrandire']//@src").extract()]
        if images:
                item_loader.add_value("images", images)

        item_loader.add_value("landlord_phone", "091 6826773")
        item_loader.add_value("landlord_email", "info@direzionecasa2017.it")
        item_loader.add_value("landlord_name", "Direzione Casa")

        yield item_loader.load_item()