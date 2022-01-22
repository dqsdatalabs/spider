# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from itemadapter.utils import is_scrapy_item
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re

class MySpider(Spider): 
    name = 'amstel-property_com'
    execution_type='testing'
    country='netherlands'
    locale='nl'
    external_source="Amstelproperty_PySpider_netherlands"
    custom_settings = {
        "HTTPCACHE_ENABLED": False
    }
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://amstel-property.com/huurwoningen/?_types=cb6fcd2fc65ede34af0894915a48c50f",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://amstel-property.com/huurwoningen/?_types=geschakelde-woning",
                    "https://amstel-property.com/huurwoningen/?_types=hoekwoning",
                    "https://amstel-property.com/huurwoningen/?_types=tussenwoning",
                    "https://amstel-property.com/huurwoningen/?_types=vrijstaande-woning"
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,callback=self.parse,meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        seen = False

        for item in response.xpath("//div[@class='object-image position-relative mb-3']/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta.get("property_type")}) 
            seen = True
        if page == 2 or seen:
            next_page = f"{response.url}&_paged={page}" 
            if next_page:
                yield Request(url=response.urljoin(next_page),callback=self.parse, meta={"page": page+1,"property_type":response.meta.get("property_type")}) 

    
    
    # 2. SCRAPING level 2 
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title","//title//text()")

        adres=" ".join(response.xpath("//div[@class='object-detail-adres mt-3']//span//span//text()").getall())
        if adres:
            item_loader.add_value("address",adres)
        city=response.xpath("//div[@class='object-detail-adres mt-3']//span[2]//span[2]//text()").get()
        if city:
            item_loader.add_value("city",city)
        zipcode=response.xpath("//div[@class='object-detail-adres mt-3']//span[2]//span[1]//text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode)

        description="".join(response.xpath("//div[@class='object-information']//p//text()").getall())
        if description:
            item_loader.add_value("description",description)
        images=[x for x in response.xpath("//div[@class='object-detail-photos object-detail-photos-v1']//img//@data-src").getall()]
        if images:
            item_loader.add_value("images",images)
        square_meters=response.xpath("//div[contains(.,'Oppervlakte')]/parent::div/following-sibling::div/div/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.replace("\n","").replace("\t","").split("m")[0])
        room_count=response.xpath("//div[contains(.,'Aantal kamers')]/parent::div/following-sibling::div/div/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.strip().split(" ")[0])
        energy_label=response.xpath("//div[contains(.,'Energielabel')]/parent::div/following-sibling::div/div/text()").get()
        if energy_label:
            item_loader.add_value("energy_label",energy_label.strip())
        name=response.xpath("//div[@class='contact-info']/h5/text()").get()
        if name:
            item_loader.add_value("landlord_name",name)
        phone=response.xpath("//div[@class='contact-info']//li[@class='li-contact-info-phone text-truncate']/a/@href").get()
        if phone:
            item_loader.add_value("landlord_phone",phone.split(":")[-1])
        email=response.xpath("//div[@class='contact-info']//li[@class='li-contact-info-email text-truncate']/a/@href").get()
        if email:
            item_loader.add_value("landlord_email",email.split(":")[-1])

        rented_cond = response.xpath("//div[@class='object-status position-absolute py-2 px-3']").get()
        if rented_cond:
            return

        bathroom_count = response.xpath("//div[@class='object-feature-info text-truncate'][contains(text(),'badkamers')]/text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip().split()[0]
            item_loader.add_value("bathroom_count",bathroom_count)

        item_loader.add_value("currency","EUR")

        rent = response.xpath("//span[@class='object-price-value']/text()").get()
        if rent:
            rent = rent.replace("€","").replace(".","").strip()
            item_loader.add_value("rent",rent)

        external_id = (response.url).strip("/").split("-")[-1]
        item_loader.add_value("external_id",external_id)  

        parking = response.xpath("//h3[text()='Parkeergelegenheid']").get()
        if parking:
            item_loader.add_value("parking",True)  

        yield item_loader.load_item() 