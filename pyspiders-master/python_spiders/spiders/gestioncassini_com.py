# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.http import headers
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import math
import re

class MySpider(Spider):
    name = 'gestioncassini_com'
    execution_type='testing'                          
    country='france'
    locale='fr'
    external_source='Gestioncassini_PySpider_france_fr'
    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "en,tr-TR;q=0.9,tr;q=0.8,en-US;q=0.7",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.61 Safari/537.36",
    }
    def start_requests(self):
        start_urls = [
            {"url": "https://www.gestioncassini.com/fr/recherche", "property_type": "apartment"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//figure"):
            base_url = "https://www.gestioncassini.com"
            follow_url = base_url + item.xpath("./a/@href").extract_first()
            yield Request(follow_url, callback=self.populate_item, headers=self.headers, meta={'property_type': response.meta.get('property_type')})
            
# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        status="".join(response.xpath("//link[@rel='canonical']//@href").get())
        if "vente" not in status.lower() and "commercial" not in status:
            item_loader.add_value("external_link", response.url)
            item_loader.add_value("external_id", response.url.split("+")[-1])
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value("property_type", response.meta.get('property_type'))
            
            title = response.xpath("//title//text()").get()
            if title:
                item_loader.add_value("title", title)

            item_loader.add_value("city", "NICEA")
            item_loader.add_value("address", "NICEA")

            price = response.xpath("//div[@class='module module-90449 module-property-info property-info-template-18 ']//ul//li[contains(.,'€ / Mois')]//text()").get()
            if price:
                price=price.split("€")[0]
                item_loader.add_value("rent", price.replace(" ",""))
            item_loader.add_value("currency", "EUR")

            square = response.xpath("//div[@class='module module-90449 module-property-info property-info-template-18 ']//ul//li[contains(.,'m²')]//text()").get()
            if square:
                square=square.split("m")[0]
                item_loader.add_value("square_meters", square.split(".")[0])

            desc = "".join(response.xpath("//p[@id='description']//text()").getall())
            if desc:
                item_loader.add_value("description", desc)

            utilities = response.xpath("//ul//li[contains(.,'Provision sur charges récupérables')]//span//text()").get()
            if utilities:
                item_loader.add_value("utilities", utilities.split("€")[0])

            room_count = response.xpath("//div[@class='module module-90449 module-property-info property-info-template-18 ']//ul//li[contains(.,'chambre')]//text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.split("chambre"))

            bathroom_count = response.xpath("//div[@class='module module-90449 module-property-info property-info-template-18 ']//ul//li[contains(.,'bains')]//text()").get()
            if bathroom_count:
                item_loader.add_value("bathroom_count", bathroom_count.split("salle de bains"))

            images = [response.urljoin(x)for x in response.xpath("//img[contains(@class,'img-lazy-load')]//@data-src").extract()]
            if images:
                    item_loader.add_value("images", images)

            item_loader.add_value("landlord_phone", "+33 4 93 26 17 18")
            item_loader.add_value("landlord_email", "contact@gestioncassini.com")
            item_loader.add_value("landlord_name", "Gestion Cassini")
            
            yield item_loader.load_item()