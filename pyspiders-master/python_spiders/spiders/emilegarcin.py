# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json


class EmilegarcinSpider(Spider):
    name = "emilegarcin"
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    thousand_separator = '.'
    scale_separator = ','
    external_source="Emilegarcin_PySpider_france_fr"

    def start_requests(self):
        start_urls = [
            {"url": [ "https://www.emilegarcin.com/fr/annonces/location?page=1",],
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse
                
                )

    def parse(self, response):
        headers={
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36",
        }
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//div[@class='btns-container']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item,headers=headers)
            seen = True
        
        if page == 2 or seen:
            url = f"https://www.emilegarcin.com/fr/annonces/location?page={page}"
            yield Request(url, callback=self.parse, meta={"page": page+1},headers=headers)

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        # item_loader.add_value("property_type", property_type)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        property_type = response.xpath("//h1[@class='title-md c-dark-brown mb-sm']//text()").get()
        if property_type and "appartement" in property_type.lower():
            item_loader.add_value("property_type", "apartment")
        else:
            item_loader.add_value("property_type", "house")

        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)

        external_id=response.xpath("//div[@class='mb-sm']//div[contains(.,'Référence')]//following-sibling::div//text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)

        rent=response.xpath("(//div[contains(.,'Prix')]//div[@class='c-dark-brown fw-700 fs-lg lh-xs'])[1]/text()").get()
        if rent:
            rent = rent.split("€")[0]
            if rent and " " in rent:
                rent = rent.replace(" ","")
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","EUR")

        square_meters=response.xpath("//div[contains(@itemprop,'size')]/text()").get()
        if square_meters:
            square_meters = square_meters.split("m²")[0]
            item_loader.add_value("square_meters",square_meters)  

        description=response.xpath("//div[contains(@class,'cms cms-no-break')]/p/text()").get()
        if description:
            item_loader.add_value("description",description)  

        address=response.xpath("(//div[@class='mb-md fs-md']//div//text())[1]").get()
        if address:
            if address and " " in address:
                address = address.split(" ")[0]
            item_loader.add_value("address",address)  
            item_loader.add_value("city",address)  

        room_count=response.xpath("//div[@class='mb-md']//div[contains(.,'Nombre de chambres ')]//following-sibling::div[@class='fw-700']//text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)

        images = [response.urljoin(x) for x in response.xpath("//div[@class='item-amp-img']//@style").getall()]
        if images:
            images = "".join(images)
            images = "".join(images.split("url('")[1].split("')")[0])
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        landlord_phone=response.xpath("//span[@class='click-content-data d-none']/text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)
        landlord_name=response.xpath("//div[@class='fw-600 c-dark-brown']/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)   
        item_loader.add_value('landlord_email', 'provence@emilegarcin.com')
        
        yield item_loader.load_item()