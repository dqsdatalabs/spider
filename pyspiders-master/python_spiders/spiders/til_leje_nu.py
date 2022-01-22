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
    name = 'til_leje_nu'
    execution_type = 'testing'
    country = 'denmark'
    locale ='da'

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.til-leje.nu/boliger/l%C3%B8gstrup/",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.til-leje.nu/boliger/viborg/"
                ],
                "property_type": "house"
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
        
        for item in response.xpath("//a[contains(@id,'boligimg')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Tillejenu_PySpider_denmark")
        externalid=response.url
        if "viborg/" in externalid:
            item_loader.add_value("external_id",externalid.split("viborg/")[-1].split("-")[0])
        if "https://www.til-leje.nu/boliger/l%c3%b8gstrup/158-borgergade-6-1-tv/"==externalid:
            item_loader.add_value("external_id","158")
        


        title = response.xpath("//span[@id='text_overskrift_stor_INDEX_0']/text()").get()
        item_loader.add_value("title", title)
        
        address = "".join(response.xpath("//div[@class='col-xs-12']//h2//text()").getall())
        if address:
            item_loader.add_value("address", address.strip())
        
        zipcode = response.xpath("//div[@class='col-xs-12']//h2[2]//text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.split(" ")[0])
            item_loader.add_value("city", zipcode.split(" ")[1])
        
        description = "".join(response.xpath("//span[@id='text_beskrivelse_INDEX_0']//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        square_meters = response.xpath("//div[label[contains(.,'Størrelse')]]//span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split(" ")[0])
            item_loader.add_value("room_count", square_meters.split("/")[-1])
        
        rent = response.xpath("//div[label[contains(.,'Leje pr. md.')]]//span/text()").get()
        if rent:
            item_loader.add_value("rent", rent.replace(".",""))
        item_loader.add_value("currency", "EUR")
        
        balcony = response.xpath("//div[label[contains(.,'Altan')]]//span/text()[contains(.,'Ja')]")
        if balcony:
            item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//div[label[contains(.,'Terrasse')]]//span/text()[contains(.,'Ja')]")
        if terrace:
            item_loader.add_value("terrace", True)
        
        pets_allowed = response.xpath("//div[label[contains(.,'Terrasse')]]//span/text()[contains(.,'Ja')]")
        if pets_allowed:
            item_loader.add_value("pets_allowed", True)
        
        deposit = response.xpath("//div[label[contains(.,'Depositum')]]//span/text()").get()
        if deposit:
            deposit = deposit.split("(")[1].split(" ")[0].replace(".","")
            item_loader.add_value("deposit", deposit)
        
        images = [response.urljoin(x) for x in response.xpath("//a[contains(@id,'boligimg_thumb_INDEX')]/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        available_date=response.xpath("//label[@id='text_ledigpr_INDEX_0_lbl']/../div/span/text()").get()
        if available_date:
            item_loader.add_value("available_date",available_date)
        
        item_loader.add_value("landlord_name", "Viborg Bolig- & Erhvervsudlejning ApS")
        item_loader.add_value("landlord_phone", "+45 4343 9999")
        item_loader.add_value("landlord_email", "mail@til-leje.nu")
        
        yield item_loader.load_item()