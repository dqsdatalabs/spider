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
    name = 'istanbulestate_co'
    execution_type='testing'
    country='turkey'
    locale='en'
    external_source="Istanbulestate_PySpider_turkey_en"
    
    
    def start_requests(self):
        start_urls = [
            {
                "url" : "https://www.istanbulestate.co/property-rent-istanbul-long-term-apartments/",
                "property_type" : "apartment"
            },
        ]
        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[contains(@class,'property_listing ')]/h4/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

        
        next_page = response.xpath("//i[contains(@class,'right')]/../@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)
        title = response.xpath("//h1[@class='entry-title']/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
   

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        
        external_id=response.xpath("//div[@class='panel-body']/div[contains(.,'Id')]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        
        desc=" ".join(response.xpath("//div[@class='wpb_wrapper']/div[contains(@class,'wpestate_estate_property_details')]/p//text()").getall())
        if desc:
            item_loader.add_value("description", desc.replace(" \u00a0","").replace(" \xa0",""))
            if "Pets are not allowed" in desc:
                item_loader.add_value("pets_allowed", False)
        rent="".join(response.xpath("//div[@class='panel-body']/div[contains(.,'Price')]/text()").getall())
        if rent:
            item_loader.add_value("rent_string", rent)
        
        deposit="".join(response.xpath("//div[@class='wpb_wrapper']/div/p[contains(.,'Deposit')]//text()").getall())
        if deposit:
            deposit=deposit.split(':')[1].split('TL')[0].strip().replace(",",".")
            item_loader.add_value("deposit", deposit)
        
        square_meters=response.xpath("//div[@class='panel-body']/div[contains(.,'Size')]/text()").get()
        square_meters2=response.xpath("//div[contains(@class,'details_section')]/p[contains(.,'Size')]/span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split('m')[0].split(",")[0].strip())
        elif square_meters2:
            item_loader.add_value("square_meters", square_meters2.split("m2")[0].replace(":","").strip())
        elif "m2" in desc:
            sq_meter=desc.split("m2")[0].strip().split(" ")[-1].replace("(","")
            if sq_meter.isdigit():
                item_loader.add_value("square_meters", sq_meter)
        
        # room=response.xpath("//div[@class='wpb_wrapper']/div/*[not(self::span) and self::p][contains(.,'bedroom')][1]/text()").get()
        # rooms=response.xpath("//div[@class='panel-body']/div[contains(.,'Room')]/text()").get()
        
        
        # if rooms :
        #     living_room=response.xpath("//div[@class='wpb_wrapper']//div//p//span//strong[contains(.,'Apartment type')]//following-sibling::text()[contains(.,'living')]").get()
        #     if living_room:
        #         living_room=living_room.split("+")[1].split("living")[0]
        #         total_room=int(rooms)+int(living_room) 
        #         item_loader.add_value("room_count", total_room)
        # elif room:
        #     item_loader.add_value("room_count", room.split("bedroom")[0].strip().split(" ")[-1])

        room_count=response.xpath("//div[@class='wpb_wrapper']//div//p//span//strong[contains(.,'Apartment type')]//following-sibling::text()").get()
        if room_count:
            room_count=room_count.split(":")[-1].replace("\xa0","")
            room1=room_count.split("+")[0].split("bed")[0]
            room2=room_count.split("+")[1].split("liv")[0]
            item_loader.add_value("room_count",int(room1)+int(room2))
        roomcheck=item_loader.get_output_value("room_count")
        if not roomcheck:
            room3=response.xpath("//strong[.='Rooms:']/following-sibling::text()").get()
            room4=response.xpath("//strong[.='Bedrooms:']/following-sibling::text()").get()
            if room3 and room4:
                item_loader.add_value("room_count",int(room3)+int(room4))

        bath_room=response.xpath("//div[@class='panel-body']/div[contains(.,'Bathrooms')]/text()").get()
        if bath_room :
            item_loader.add_value("bathroom_count", bath_room)

        utilities="".join(response.xpath("//div[contains(@class,'wpestate_estate_property_details_section')]//p/span[contains(.,'Monthly maintenance fee')]/text()[contains(.,'TL')]").getall())
        if utilities :
            item_loader.add_value("utilities", utilities.split(":")[1].strip())

        floor= "".join(response.xpath("//div[contains(@class,'wpestate_estate_property_details_section')]//p//strong[contains(.,'Floor')]/following-sibling::text()").getall())
        if floor :
            item_loader.add_value("floor", floor.split(":")[1].strip())

        address=", ".join(response.xpath("//div[@class='wpb_wrapper']/div/p/a/text()[normalize-space()]").getall())
        if address:
            item_loader.add_value("address", address.strip())
        
        city=response.xpath("//div[@class='wpb_wrapper']/div/p/span[.='City:']/following-sibling::a[1]/text()").get()
        if city:
            item_loader.add_value("city", city)
        
        desc="".join(response.xpath("//div[@class='wpb_wrapper']/div/*[not(self::span) and self::p]//text()").getall())
        if desc:
            item_loader.add_value("description", desc)
        
        furnished="".join(response.xpath(
            "//div[@class='wpb_wrapper']/div/p[contains(.,'Furniture')]//text()").getall())
        if "Furnished" in furnished:
            item_loader.add_value("furnished", True)

            
        images=[x for x in response.xpath("//div[@id='carousel-listing']/ol/li/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        latitude_longitude=response.xpath("//script[@id='googlecode_property-js-extra']/text()").get()
        if latitude_longitude:
            lat=latitude_longitude.split('latitude":"')[1].split('",')[0]
            lng=latitude_longitude.split('longitude":"')[1].split('",')[0]
            item_loader.add_value("latitude",lat)
            item_loader.add_value("longitude",lng)
            
        parking=response.xpath("//div[@class='panel-body']/div[contains(.,'Car Park')]/text()").get()
        if parking:
            item_loader.add_value("parking",True)
            
        swimming_pool=response.xpath("//div[@class='panel-body']/div[contains(.,'Pools')]/text()").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool",True)

        item_loader.add_value("landlord_phone", " +90 532 483 31 46")
        item_loader.add_value("landlord_email", "info@istanbulestate.co")
        item_loader.add_value("landlord_name", "Istanbul Estate Co.")

        yield item_loader.load_item()
