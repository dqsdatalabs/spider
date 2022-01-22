# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector 
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re
import dateparser

class MySpider(Spider):
    name = 'axellenaerts_be'
    execution_type = 'testing'
    country = 'belgium'
    locale = 'nl'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.axellenaerts.be/api/properties.json?pg=1&mapsView=false&state=57&type=159",
                    "https://www.axellenaerts.be/api/properties.json?pg=1&mapsView=false&state=57&type=556"
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.axellenaerts.be/api/properties.json?pg=1&mapsView=false&state=57&type=491",
                ],
                "property_type" : "house"
            }
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})
    
    def parse(self, response):
        data = json.loads(response.body)

        for item in data["data"]:
            url = item["url"]
            id = item["id"]
            city = item["city"]
            yield Request(url, callback=self.populate_item, meta={"property_type": response.meta["property_type"],"id":id,"city":city})

        next_button = data["meta"]["pagination"]["links"]
        if next_button and "next" in next_button: 
            follow_url = next_button["next"]
            yield Request(follow_url, callback=self.parse, meta={"property_type": response.meta["property_type"]})
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "AxelLenaerts_PySpider_belgium")
        item_loader.add_value("external_id", str(response.meta.get("id")))
        item_loader.add_xpath("title", "//div/h1[@class='s-title-with-handwriting__title']//text()")

        property_type =response.meta.get("property_type")
        item_loader.add_value("property_type", property_type)

        city =response.meta.get("city")
        item_loader.add_value("city", city)

        address = response.xpath("//div[@class='s-property-detail__location-links']/a[1]/text()[normalize-space()]").get()
        if address:
            zipcode = address.split(",")[-1].strip().split(" ")[0]
            item_loader.add_value("address", address.strip())
            item_loader.add_value("zipcode", zipcode.strip())
        else:
            item_loader.add_value("address", city)

        price = response.xpath("//div[div[.='Maandelijkse huurprijs']]/div[2]/text()").get()
        if price:
            item_loader.add_value("rent_string", price.strip())

        utilities = response.xpath("//div[div[.='Gemeenschappelijke kosten']]/div[2]/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.strip())
        item_loader.add_xpath("room_count", "//div[div[.='Slaapkamers']]/div[@class='s-bold']/text()")
        item_loader.add_xpath("bathroom_count", "//div[div[.='Badkamers']]/div[@class='s-bold']/text()")
        
        meters = response.xpath("//div[div[.='Bewoonbare opp.']]/div[@class='s-bold']/text()").get()
        if meters:
            item_loader.add_value("square_meters",meters.split("m")[0])

        description = " ".join(response.xpath("//div[@class='s-container-inset-text']//text()[normalize-space()]").getall())
        if description:
            item_loader.add_value("description", description.strip())


        terrace =  response.xpath("//div[div[.='Terras']]/div[@class='s-bold']/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)   
        parking =  response.xpath("//div[div[.='Garages']]/div[@class='s-bold']/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        item_loader.add_xpath("energy_label", "//div[div[.='EPC Categorie']]/div[@class='s-bold']/text()[.!='Onbekend']")
        available_date=response.xpath("//span[contains(.,'Beschikbaar vanaf')]/text()").get()
        if available_date:
            date=available_date.split("vanaf")[-1].strip()
            if date:
                item_loader.add_value("available_date",date.split(".")[0])
        lat_lng = response.xpath("//div[@class='s-property-detail__location-links']/a[2]/@href").get()
        if lat_lng:
            lat_lng = lat_lng.split("&viewpoint=")[-1]
            item_loader.add_value("latitude", lat_lng.split(",")[0])
            item_loader.add_value("longitude", lat_lng.split(",")[1])

        images = [response.urljoin(x) for x in response.xpath("//div//a[@data-fslightbox='property-intro-lightbox']/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            
        lat_lng = response.xpath("//div[@class='s-property-detail__contact-agent__info']/div[@class='s-bold'][1]/text()").get()
        if lat_lng:
            item_loader.add_xpath("landlord_name", "//div[@class='s-property-detail__contact-agent__info']/div[@class='s-bold'][1]/text()")       
            item_loader.add_xpath("landlord_phone", "//div[@class='s-property-detail__contact-agent__info']/a[contains(@href,'tel:')]/text()")       
            # item_loader.add_xpath("landlord_email", "//div[@class='s-property-detail__contact-agent__info']/a[contains(@href,'mail')]/text()")      
        else:
            item_loader.add_value("landlord_name", "Axel Lenaerts")       
            item_loader.add_value("landlord_phone", "09 245 24 24")       
        item_loader.add_value("landlord_email", "info@axellenaerts.be")       
    
        yield item_loader.load_item()

