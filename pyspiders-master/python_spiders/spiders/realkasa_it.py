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
import math
class MySpider(Spider):
    name = 'realkasa_it'
    execution_type='testing'
    country='italy'
    locale='it' 
    external_source = "Realkasa_PySpider_italy"

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.realkasa.it/immobili/?r=1&order_by=&rental=1&rooms=0&size_min=&size_max=&price_min=&price_max=&price_min_af=&price_max_af=&tags3_1=&tags_control=&bathrooms=&categories_multi%5B0%5D=0&typology_multi%5B0%5D=1&company_id=&coords_zoom=&coords=&pg=1",
                ],      
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "https://www.realkasa.it/immobili/?order_by&r=1&rental=1&company_id&seo&categories_id&coords&coords_center&coords_zoom&tags3_1&code&size_min&size_max&price_min&price_max&price_min_af&price_max_af&one=on&categories_multi%5B0%5D=0&typology_multi%5B0%5D=11&rooms=0&bedrooms&bathrooms",

                ],
                "property_type" : "house" 
            },
            {
                "url" : [
                    "https://www.realkasa.it/immobili/?order_by&r=1&rental=1&company_id&seo&categories_id&coords&coords_center&coords_zoom&tags3_1&code&size_min&size_max&price_min&price_max&price_min_af&price_max_af&one=on&categories_multi%5B0%5D=0&typology_multi%5B0%5D=1&rooms=1&bedrooms&bathrooms",

                ],
                "property_type" : "studio" 
            }
          
        ] 
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='image-box']"):
            url = item.xpath("./a/@href").get()
            # rented = item.xpath("//span[@class='featured']//text()[contains(.,'In affitto')]").get()
            # if rented:
            #     return
            yield Request(url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        pagination = response.xpath("//div[@class='styled-pagination']//li[@class='next']//a//@href").get()
        if pagination:
            yield Request(response.urljoin(pagination), callback=self.parse, meta={"property_type":response.meta["property_type"]})
        else:
            pagination = response.xpath("//div[@class='styled-pagination']//li[@class='active']/following-sibling::li[1]/a/@href").get()
            yield Request(response.urljoin(pagination), callback=self.parse, meta={"property_type":response.meta["property_type"]})
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title", "//div[@class='upper-info-box']//h2//text()")
        external_id = "".join(response.xpath("//span[@class='title']//text()").get())
        if external_id:
            item_loader.add_value("external_id", external_id.split("Rif. ")[1])
        rent = response.xpath("//div[@class='price-column col-lg-4 col-md-12 col-sm-12']/div//text()").get()
        if rent:
            item_loader.add_value("rent", rent.split("€ ")[1])  
        item_loader.add_value("currency", "EUR")   

        room_count = response.xpath("(//ul[@class='list-style-one']/li[contains(.,'Locali:')]//b//text())[1]").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//i[@class='fa fa-bed']/parent::li//text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.split('Camere')[0].split('Camera')[0].strip())
            else:
                item_loader.add_value("room_count", "1")

        address = response.xpath("//div[@class='location']/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            if address and "," in address:
                city = address.split(',')[0].strip()
                if city:
                    item_loader.add_value("city", city)
            else:
                item_loader.add_value("city", address.strip())
        
        bathroom_count = response.xpath("//i[@class='fas fa-bath']/parent::li//text()").get()
        if bathroom_count:
                item_loader.add_value("bathroom_count", bathroom_count.split('Bagno')[0].split('Bagni')[0].strip())

        floor = response.xpath("//i[@class='fas fa-building']/parent::li//text()").get()
        if floor:
            item_loader.add_value("floor", floor.split('Piano')[1].strip())

        utilities = response.xpath("//ul[@class='list-style-one']/li[contains(.,'Spese Annuali')]/b/text()").get()
        if utilities:
            utilities=utilities.split('€')[-1].replace(".","")
            utilities=int(float(utilities)/12)
            item_loader.add_value("utilities",utilities)

        square = response.xpath("//ul[@class='list-style-one']/li[contains(.,'MQ')]/b/text()").extract_first()
        if square:
            square_meters = math.ceil(float(square.strip()))
            item_loader.add_value("square_meters",square_meters)
        
        energy_label = response.xpath("//ul[@class='list-style-one']/li[contains(.,'Classe Energ')]/b/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)
        
        furnished = "".join(response.xpath("(//ul[@class='list-style-one']/li[contains(.,'Arredato')]//text())[1]").getall())
        if furnished:
            item_loader.add_value("furnished",True)
        
        elevator = response.xpath("(//ul[@class='list-style-one']/li[contains(.,'Ascensore')]//text())[1]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        balcony = response.xpath("(//ul[@class='list-style-one']/li[contains(.,'Balcone')]//text())[1]").get()
        if balcony:
            item_loader.add_value("balcony", True)

        dishwasher = response.xpath("(//ul[@class='list-style-one']/li[contains(.,'Inferriate')]//text())[1]").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)

        washing_machine = response.xpath("(//ul[@class='list-style-one']/li[contains(.,'Lavatrice')]//text())[1]").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)

        parking = response.xpath("//ul[@class='list-style-one']/li/text()[contains(.,'Parcheggio')]").get()
        if parking:
            item_loader.add_value("parking", True)
        else:
            parking = response.xpath("//ul[@class='list-style-one']/li/text()[contains(.,'Posti Auto')]").get()
            if parking:
                item_loader.add_value("parking", True)

        desc = "".join(response.xpath("//div[@class='lower-content']//p//text()").extract())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc.strip())

        images = [x for x in response.xpath("//div[@class='carousel-outer']//ul//li//a//@href").extract()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))

        floor_plan_images = [x for x in response.xpath("//figure[@class='image']/img//@src").extract()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        latlng = response.xpath("//div[@id='mappa']//script[contains(.,'LatLng')]//text()").get()
        if latlng:
            item_loader.add_value("latitude", latlng.split('LatLng(')[1].strip().split(',')[0])
            item_loader.add_value("longitude", latlng.split(',')[1].strip().split(')')[0])

        landlord_name=response.xpath("//h4[@class='name']//text()").extract_first()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)

        landlord_email=response.xpath("//div[@class='info-box']//a[@class='mail']//text()").extract_first()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email)

        landlord_phone=response.xpath("//div[@class='info-box']//a//i[@class='la la-phone']//parent::a//text()").extract_first()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)

        yield item_loader.load_item()