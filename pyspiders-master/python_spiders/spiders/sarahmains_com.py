# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
from python_spiders.helper import extract_number_only, remove_white_spaces
from word2number import w2n
import lxml
import js2xml

class MySpider(Spider):
    name = 'sarahmains_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source = "Sarahmains_PySpider_united_kingdom_en"

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.sarahmains.com/search?limit=20&includeDisplayAddress=Yes&active=&auto-lat=&auto-lng=&p_department=RL&location=&propertyType=8%2C11%2C28&minimumRent=&maximumRent=&minimumBedrooms=0&maximumBedrooms=0&searchRadius=&availability=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.sarahmains.com/search?limit=20&includeDisplayAddress=Yes&active=&auto-lat=&auto-lng=&p_department=RL&location=&propertyType=1%2C2%2C3%2C26&minimumRent=&maximumRent=&minimumBedrooms=0&maximumBedrooms=0&searchRadius=&availability=",
                    "https://www.sarahmains.com/search?limit=20&includeDisplayAddress=Yes&active=&auto-lat=&auto-lng=&p_department=RL&location=&propertyType=14&minimumRent=&maximumRent=&minimumBedrooms=0&maximumBedrooms=0&searchRadius=&availability="                
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='search-results-gallery-property']/a[1]"):
            follow_url = response.urljoin(item.xpath(".//@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        pagination = response.xpath("//ul[@class='pagination']/li[@class='active']/following-sibling::li[1]/a/@href").get()
        if pagination:
            yield Request(
                response.urljoin(pagination),
                callback=self.parse,
            )       
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        status_check = response.xpath("//span[@class='availability ml-20']/text()[contains(.,'Let Agreed')]").get()
        if status_check:
            return

        if response.xpath("//ul[@class='property_features']/li/text()[contains(.,'Student')]").get():
            property_type = "student_apartment"
        elif response.xpath("//ul[@class='property_features']/li/text()[contains(.,'Studio')]").get():
            property_type = "studio"
        else:
            property_type = response.meta.get('property_type')
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("external_link", response.url)

        title = response.xpath("(//title/text())[1]").get()
        if title:
            item_loader.add_value("title", title)
        
        external_id = response.xpath("//span/text()[contains(.,'Ref:')]").get()
        if external_id:
            external_id = external_id.split(':')[1].strip()
            item_loader.add_value("external_id", external_id)

        rent = response.xpath("//span[@class='price']/text()").get()
        if rent and "pcm" in rent.lower():
            rent = rent.replace('£','').replace('pcm','').replace(',','').strip()
            item_loader.add_value("rent", rent)
        elif rent and "pw" in rent.lower():
            rent = rent.replace('£','').replace('pw','').replace(',','').strip()
            item_loader.add_value("rent", int(rent)*4)
        item_loader.add_value("currency", "GBP")

        room_count = response.xpath("//span[@class='type']/text()").get()
        if room_count and "bedroom" in room_count.lower():
            room_count = room_count.split('Bedroom')[0].split('bedroom')[0].strip()
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//ul[@class='property_features']/li/text()[contains(.,'Bathroom')]").get()
        if bathroom_count:
            bathroom_count = w2n.word_to_num(bathroom_count)
            item_loader.add_value("bathroom_count", bathroom_count)

        energy_label = response.xpath("//ul[@class='property_features']/li/text()[contains(.,'EPC')]").get()
        if energy_label:
            energy_label = energy_label.split('EPC')[1].split('Rating')[1].strip()
            item_loader.add_value("energy_label", energy_label)

        address = response.xpath("//div[contains(@class,'property-details-box')]/h1/text()").get()
        if address:
            item_loader.add_value("address", address)
            zipcode = address.split(',')[-1].strip()
            if zipcode:
                item_loader.add_value("zipcode", zipcode)
            
        item_loader.add_value("city", "Newcastle")
        
        desc = "".join(response.xpath("//div[@class='full_description_large']/text()").getall())
        if desc:
            item_loader.add_value("description", desc.replace('\n', ' '))
        else:
            desc = "".join(response.xpath("//div[@class='full_description_small']/text()").getall())
            if desc:
                item_loader.add_value("description", desc.replace('\n', ' '))
        
        furnished = response.xpath("//ul[@class='property_features']/li/text()[contains(.,'Furnished')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        parking = response.xpath("//ul[@class='property_features']/li/text()[contains(.,'Garage')]").get()
        if parking:
            item_loader.add_value("parking", True)
        else:
            parking = response.xpath("//ul[@class='property_features']/li/text()[contains(.,'Parking')]").get()
            if parking:
                item_loader.add_value("parking", True)
        
        balcony = response.xpath("//ul[@class='property_features']/li/text()[contains(.,'Balcony')]").get()
        if balcony:
            item_loader.add_value("balcony", True)

        javascript = response.xpath("//script/text()[contains(.,'thumbnail_images')]").extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            selector = Selector(text=xml)
            images = selector.xpath('.//property[@name="image"]/string/text()').extract()
            if images:
                item_loader.add_value("images", images)
                item_loader.add_value("external_images_count", len(images))

        floor_plan_images = response.xpath("//div[@class='thumbnail floorplan']/@style").get()
        if floor_plan_images:
            floor_plan_images = floor_plan_images.split("url('")[1].split("')")[0].strip()
            item_loader.add_value("floor_plan_images", floor_plan_images)


        item_loader.add_value("landlord_name", "Sarah Mains Residential Sales & Lettings")
        landlord_phone = response.xpath("//div[@class='office_details']/p/a/strong/text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)
        else:
            item_loader.add_value("landlord_phone", "0191 240 3333")
        landlord_email = response.xpath("//div[@class='office_details']/a/strong/text()").get()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email)
        else:
            item_loader.add_value("landlord_email", "js@sarahmains.com")    
       
        yield item_loader.load_item()
