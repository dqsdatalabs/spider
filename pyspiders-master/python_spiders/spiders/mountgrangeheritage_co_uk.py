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
    name = 'mountgrangeheritage_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    def start_requests(self):

        start_urls = [
            {
                "type" : "HOUS&st=MEWS&st=PFH",
                "property_type" : "house"
            },
            {
                "type" : "CONVF&st=FLAT&st=MAIS&st=PBF&st=PFF",
                "property_type" : "apartment"
            },
        ]
        for url in start_urls:

            formdata = {
                "ba": "0",
                "od": "DP",
                "cs": "",
                "te": "",
                "fu": "",
                "ou": "",
                "rd": "",
                "de": "RL",
                "st": url.get("type"),
                "be": "0",
                "pf": "0",
                "pt": "99999",
            }

            yield FormRequest(
                url="https://www.mountgrangeheritage.co.uk/search/search-results.php",
                callback=self.parse,
                formdata=formdata,
                meta={'property_type': url.get('property_type')}
            )

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='leadphoto']"):
            if item.xpath("./div[@id='status']/text()").get() and item.xpath("./div[@id='status']/text()").get() in ["Under Offer"]:
                continue
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})  
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Mountgrangeheritage_Co_PySpider_united_kingdom")

        external_id = response.url.split('?id=')[-1].strip()
        if external_id:
            item_loader.add_value("external_id", external_id)
        
        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            zipcode = address.split(',')[-1].strip()
            if " " in zipcode: zipcode = zipcode.split(" ")[-1]
            item_loader.add_value("zipcode", address.split(',')[-1].strip())
        
        city_info = response.xpath("//p[contains(@class,'text-purple')]/text()").get()
        if city_info and "in" in city_info:
            item_loader.add_value("city", city_info.split("in")[1].strip())
            
        title = response.xpath("//div[@class='col-12 col-md-7 mb-5 mb-md-0']/p[1]/text()").get()
        if title:
            item_loader.add_value("title", title.strip().replace('\xa0', ''))

        description = " ".join(response.xpath("//div[@class='col-12 col-md-7 mb-5 mb-md-0']/h2/following-sibling::*//text()").getall()).strip()
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))
        
        square_meters = response.xpath("//li[contains(.,'square feet')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", str(int(float(square_meters.split('square')[0].replace(",","")) * 0.09290304)))

        room_count = response.xpath("//img[contains(@src,'bed-icon')]/following-sibling::h3/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//img[contains(@src,'bath-icon')]/following-sibling::h3/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        rent = response.xpath("//span[@id='price']/text()").get()
        term = response.xpath("//span[@class='suffix']/text()").get()
        if rent and term:
            if 'pppw' in term or 'per week' in term:
                rent = rent.split('£')[-1].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent)) * 4))
                item_loader.add_value("currency", 'GBP')
            elif 'pcm' in term:
                rent = rent.split('£')[-1].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent))))
                item_loader.add_value("currency", 'GBP')
        
        images = [response.urljoin(x) for x in response.xpath("//div[@id='slider']/ul//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        floor_plan_images = [x for x in response.xpath("//div[@id='nav-floorplan']//img/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        latitude_longitude = response.xpath("//script[contains(.,'var latitude')]/text()").get()
        if latitude_longitude:
            item_loader.add_value("latitude", latitude_longitude.split('var latitude =')[1].split(';')[0].strip().strip("'"))
            item_loader.add_value("longitude", latitude_longitude.split('var longitude =')[1].split(';')[0].strip().strip("'"))

        pets_allowed = response.xpath("//li[contains(.,'Pet friendly')]").get()
        if pets_allowed:
            item_loader.add_value("pets_allowed", True)

        furnished = response.xpath("//div[@class='col-12 col-md-7 mb-5 mb-md-0']/text()[last()]").get()
        if furnished:
            if furnished.strip().lower() == 'furnished':
                item_loader.add_value("furnished", True)
            elif furnished.strip().lower() == 'unfurnished':
                item_loader.add_value("furnished", False)
        
        parking = response.xpath("//li[contains(.,'parking') or contains(.,'Parking')]").get()
        if parking:
            item_loader.add_value("parking", True)
        
        balcony = response.xpath("//li[contains(.,'balcony') or contains(.,'Balcony')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        elevator = response.xpath("//li[contains(.,'lift') or contains(.,'Lift')]").get()
        if elevator:
            item_loader.add_value("elevator", True)

        terrace = response.xpath("//li[contains(.,'terrace') or contains(.,'Terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        swimming_pool = response.xpath("//li[contains(.,'swimming pool') or contains(.,'Swimming pool')]").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)

        item_loader.add_xpath("landlord_name", "//div[@class='staff-thumbnail']/following-sibling::div/h3/text()")
        item_loader.add_xpath("landlord_phone", "//div[@class='staff-thumbnail']/following-sibling::div/p[contains(@class,'phone')]/a/text()")
        item_loader.add_xpath("landlord_email", "//div[@class='staff-thumbnail']/following-sibling::div/p[contains(@class,'email')]/a/text()")
        
        
        yield item_loader.load_item()
