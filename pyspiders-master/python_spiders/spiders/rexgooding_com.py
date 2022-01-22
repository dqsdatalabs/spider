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
from datetime import datetime
from datetime import date
import dateparser

class MySpider(Spider):
    name = 'rexgooding_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    start_urls = ["https://www.rexgooding.com/residential?property-filter-category=rent&filter-status=Hide%20SSTC"] #LEVEL-1
    external_source = "Rexgooding_PySpider_united_kingdom"
    # 1. FOLLOWING
    def parse(self, response):
 
        for item in response.xpath("//div[@class='w-dyn-item']"):
            status = item.xpath(".//div[@class='property-slider-price-title-text margin-6r']/text()").get()
            if status and "Rent " in status:
                url = item.xpath(".//div[@class='property-slider-image-w']/a/@href").get()
                yield Request(response.urljoin(url), callback=self.populate_item)
     

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        features = " ".join(response.xpath("//div[@class='property-description-rtb w-richtext']//text()").getall())
        if get_p_type_string(features):
            item_loader.add_value("property_type", get_p_type_string(features))
        else:
            desc = "".join(response.xpath("//div[@class='property-summary']/text()").getall())
            if get_p_type_string(desc):
                item_loader.add_value("property_type", get_p_type_string(desc))
            else:
                return

        item_loader.add_value("external_source", self.external_source )

        external_id = response.url.split('/')[-1]
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        address = response.xpath("//div[@class='section-header-text']/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("zipcode", address.split(',')[-1].strip().replace(".",""))
            item_loader.add_value("city", address.split(',')[-2].strip())
        
        title = response.xpath("//div[@class='section-header-text']/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        description = " ".join(response.xpath("//div[@class='property-description-rtb w-richtext']//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

  
        item_loader.add_xpath("room_count", "//div[@class='non-commercial-slider-icon-w']/img[contains(@src,'bed-')]/following-sibling::div[1]/text()")
        item_loader.add_xpath("bathroom_count", "//div[@class='non-commercial-slider-icon-w']/img[contains(@src,'bath-')]/following-sibling::div[1]/text()")
        item_loader.add_xpath("rent_string", "//div[@class='property-price-title-text margin-8r']//text()")
       
        meters = response.xpath("//ul/li[contains(.,'Sq')]/text()").get()
        if meters:
            s_meters = meters.split("Sq")[0].strip()
            sqm = str(int(float(s_meters) * 0.09290304))
            item_loader.add_value("square_meters", sqm)

        images = [x for x in response.xpath("//div[@class='single-slide-ci w-dyn-item w-dyn-repeater-item']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        floor_plan_images = [x for x in response.xpath("//div[@class='documents-w'][contains(.,'Floor Plan')]/a/@href[.!='#']").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        parking = response.xpath("//li[contains(.,'Parking') or contains(.,'parking')]").get()
        if parking:
            item_loader.add_value("parking", True)

        terrace = response.xpath("//li[contains(.,'Terrace') or contains(.,'terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True)

        item_loader.add_value("landlord_name", "Rex Gooding")
        item_loader.add_value("landlord_phone", "0115 945 5553")
        item_loader.add_value("landlord_email", "info@rexgooding.com")
      
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "cottage" in p_type_string.lower() or "terrace" in p_type_string.lower() or "home" in p_type_string.lower()):
        return "house"
    else:
        return None