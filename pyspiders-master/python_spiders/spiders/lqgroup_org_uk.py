# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from word2number import w2n 


class MySpider(Spider):
    name = 'lqgroup_org_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source = "Lqgroup_Org_PySpider_united_kingdom"
    def start_requests(self):
        start_urls = [
            {"url": "https://properties.lqhomes.com/properties-to-rent/all/", "property_type": "apartment"},            
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                        })

    # 1. FOLLOWING
    def parse(self, response):
        property_type = response.meta.get("property_type")

        for item in response.xpath("//div[@id='property_000']//a//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response): 
        item_loader = ListingLoader(response=response)
        item_loader.add_xpath("title", "//h1/text()")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source","Lqgroup_Org_PySpider_"+ self.country)
        item_loader.add_value("external_link", response.url)
        externalid=response.url
        if externalid:
            item_loader.add_value("external_id",externalid.split("/")[-1])
        address = response.xpath("//p[contains(.,'Property address')]/following-sibling::p/text()").get()
        if address:
            item_loader.add_value("address", address)
            zipcode = address.split(",")[-1].strip()
            city = address.split(zipcode)[0].strip().strip(",").split(",")[-1].strip()
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
        
        rent = response.xpath("//div[@class='header']/p/text()").get()
        if rent:
            if "POA" not in rent:
                price = rent.split("£")[1].replace(",","").strip()
                item_loader.add_value("rent", price)
            else:
                price= response.xpath("//tr[th[.='Required Household Income']]/td/text()").get()
                if price:
                    p_rent = price.split("£")[1].replace(",","").strip()
                    item_loader.add_value("rent", p_rent)
                
        
        item_loader.add_value("currency", "GBP")

        desc = "".join(response.xpath("//div[@id='detail']//p//text()").getall())
        if desc:
            desc = desc.replace("\u00a0","")
            item_loader.add_value("description", desc.strip())

        room_count = response.xpath("//div[@class='right']/p[@class='property-detail-body']/text()").get()
        if room_count:
            room_count = room_count.split("bedroom")[0].strip().split(" ")[-1]
            if room_count.isdigit():
                item_loader.add_value("room_count", room_count)
            else:
                room = response.xpath("substring-before(//div[@class='right']/p[@class='property-detail-body']/text(),',')").extract_first()
                if room:
                    r = room.split(" ")[-1].strip()
                    if r == "studio":
                        item_loader.add_value("room_count", "1")
                    else:
                        room_count = w2n.word_to_num(r)
                        item_loader.add_value("room_count", room_count)
        
        if "bathroom" in desc:
            bathroom_count = desc.split("bathroom")[0].replace("piece","").replace("\u00a0", "").strip().split(" ")[-1]
            try:
                bathroom_count = w2n.word_to_num(bathroom_count)
                item_loader.add_value("bathroom_count", bathroom_count)
            except: pass
            
        lat_lng = response.xpath("//script[contains(.,'lng')]/text()").get()
        if lat_lng:
            lat = lat_lng.split("lat':")[1].split(",")[0].strip()
            lng = lat_lng.split("lng':")[1].split(",")[0].strip()
            item_loader.add_value("latitude", lat)
            item_loader.add_value("longitude", lng)
        
        images = [ x for x in response.xpath("//div[@class='media-carousel']//img/@src").getall() ]
        if images:
            item_loader.add_value("images", images)
        
        energy_label = response.xpath("//div[@id='features']//li[contains(.,'EPC')]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split("-")[1].strip())
        
        balcony = response.xpath("//div[@id='features']//li[contains(.,'balcon') or contains(.,'Balcon')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        no_parking = response.xpath("//div[@id='features']//li[contains(.,'No parking') or contains(.,'No Parking')]/text()").get()
        parking = response.xpath("//div[@id='features']//li[contains(.,'parking') or contains(.,'Parking') or contains(.,'garage') or contains(.,'Garage')]/text()").get()
        if no_parking:
            item_loader.add_value("parking", False)
        elif parking:
            item_loader.add_value("parking", True)
        
        swimming_pool = response.xpath("//div[@id='features']//li[contains(.,'pool') or contains(.,'Pool')]/text()").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
        
        item_loader.add_value("landlord_name", "L&Q GROUP")
        item_loader.add_value("landlord_phone", "0300 456 9998")
        
        
        yield item_loader.load_item()