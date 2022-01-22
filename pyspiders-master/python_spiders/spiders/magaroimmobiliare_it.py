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
import re

class MySpider(Spider):
    name = 'magaroimmobiliare_it'
    execution_type='testing'
    country='italy'
    locale='it'
    external_source = "Magaroimmobiliare_PySpider_Italy"
    start_url = "http://www.magaroimmobiliare.com/immobili-in-affitto.html"
    Custom_settings = {
        "CONCURRENT_REQUESTS" : 4,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 1,
        "PROXY_US_ON":True,

    }

    # headers = {
    #     "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    #     "Accept-Encoding": "gzip, deflate",
    #     "Accept-Language": "en,tr-TR;q=0.9,tr;q=0.8,en-US;q=0.7",
    #     "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36",
    # }

    def start_requests(self):
        formdata = {
            "cat": "renting",
            "type_id": "1",
            "department_id": "0",
            "town_id": "0",
            "area_id": "0",
            "Itemid": "30",
            "ce16dfe19e780cb05c2826ac1f1c0b8e": "1",
        }
        yield FormRequest(self.start_url,
                        callback=self.parse,
                        #headers=self.headers,
                        # formdata=formdata,
                        # dont_filter=True,
                        meta={'property_type': "apartment"})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//dl[@class='jea_item']/dt/a/@href").getall():
            url = response.urljoin(item)
            yield Request(url, callback=self.populate_item, meta={"property_type":response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        status = response.xpath("//div[@id='main_content']/h1/text()").get()
        if "appartamento" not in status.lower():
            return

        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        external_id = response.xpath("//h2/text()[contains(.,'Rif')]").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(':')[-1].strip())
        
        title = response.xpath("//div[@id='main_content']/h1/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        rent = response.xpath("//tr/td[contains(.,'Canone affitto')]/following-sibling::td/strong/text()").get()
        if rent:
            item_loader.add_value("rent", rent.split("€")[0].replace(".","").replace(" ","").strip())
        utilities = response.xpath("//tr/td[contains(.,'Spese condomin')]/following-sibling::td/strong/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split("€")[0].replace(".","").replace(" ","").strip())
        item_loader.add_value("currency", "EUR")

        square_meters = response.xpath("//p/text()[contains(.,'Superficie coperta')]/following-sibling::strong[1]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0].strip())

        floor = response.xpath("//p/text()[contains(.,'Piano')]/following-sibling::strong[1]/text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())
        
        room_count = response.xpath("//p/text()[contains(.,'Vani')]/following-sibling::strong[1]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//p/text()[contains(.,'Bagni')]/following-sibling::strong[1]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        address = "".join(response.xpath("//h3[contains(.,'Indirizzo')]/parent::div/strong/text()").getall())
        if address:
            item_loader.add_value("address", address.strip().replace("\n","").replace("\r","").replace(" ",""))           
        
        desc = "".join(response.xpath("//div[@class='item_description']/p/span/text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
        else:
            desc = "".join(response.xpath("//div[@class='item_description']/p/text()").getall())
            if desc:
                item_loader.add_value("description", desc.strip())

        images = [x for x in response.xpath("//div[@id='jea-gallery-scroll']/a[@class='jea-thumbnails']/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        item_loader.add_value("landlord_name", "Magro immob")
        item_loader.add_value("landlord_phone", "080.5484615")
        item_loader.add_value("landlord_email", "a.magaro@alice.it")

        item_loader.add_value("external_link",response.url)
        item_loader.add_value("city","BARI")
        yield item_loader.load_item()