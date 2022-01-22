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
    name = 'lagenziaimmobiliare' 
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Lagenziaimmobiliare_PySpider_italy"
    start_urls = ['https://www.lagenziaimmobiliare.com/r/annunci/affitto-appartamento-.html?Codice=&Tipologia%5B%5D=1&Motivazione%5B%5D=2&Comune=0&Prezzo_da=&Prezzo_a=&cf=yes']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//section/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": "apartment"})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        external_id=response.xpath("//div[@class='codice']//span//text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)

        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)

        rent=response.xpath("//div[@class='schedaMobile']//div[contains(.,'Prezzo')]//strong//following-sibling::text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[1])
        item_loader.add_value("currency","EUR")

        utilities=response.xpath("//div[@class='schedaMobile']//div[contains(.,'Spese condominio')]//strong//following-sibling::text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split("€")[1])

        square_meters=response.xpath("//div[@class='schedaMobile']//div[contains(.,'Totale mq')]//strong//following-sibling::text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("mq")[0])

        address=response.xpath("//div[@class='dove_schimmo']/text()").get()
        if address:
            item_loader.add_value("address",address)

        city=response.xpath("//div[@class='schedaMobile']//div[contains(.,'Provincia')]//strong//following-sibling::text()").get()
        if city:
            item_loader.add_value("city",city)

        room_count=response.xpath("//span[contains(.,'Camere')]/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.split(" ")[0])

        bathroom_count=response.xpath("//strong[.='Bagni']/following-sibling::text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)

        description=response.xpath("//div[@class='testo']//p//text()").getall()
        if description:
            item_loader.add_value("description",description)

        images = [response.urljoin(x)for x in response.xpath("//div[contains(@class,'swiper-slide')]//img[contains(@class,'swiper-lazy')]//@data-src").extract()]
        if images:
                item_loader.add_value("images", images)

        latitude_longitude = response.xpath(
            "//script[contains(.,'lgt')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split(
                'var lat = "')[1].split('";')[0]
            longitude = latitude_longitude.split(
                'var lgt = "')[1].split('";')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_phone", "02.70105873 ")
        item_loader.add_value("landlord_email", "info@lagenziaimmobiliare.com")
        item_loader.add_value("landlord_name", "Agenzia Immobiliare")

        energy_label = response.xpath("//div[contains(@class,'new_')]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label",energy_label)

        elevator = response.xpath("//strong[text()='Ascensore']/following-sibling::text()[contains(.,'S')]").get()
        if elevator:
            item_loader.add_value("elevator",True)

        furnished = response.xpath("//strong[text()='Arredato']/following-sibling::text()[contains(.,'S')]").get()
        if furnished:
            item_loader.add_value("furnished",True)

        yield item_loader.load_item()