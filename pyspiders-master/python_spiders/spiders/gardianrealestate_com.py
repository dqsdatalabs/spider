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
class MySpider(Spider):
    name = 'gardianrealestate_com'
    execution_type='testing' 
    country='australia'
    locale='en'
    external_source = "Gardianrealestate_PySpider_australia"
    start_urls = ['https://www.gardianrealestate.com.au/rent/for-rent']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//a[contains(.,'View Listing')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
        
        pagination = response.xpath("//a[@class='page-link active']/parent::li/following-sibling::li//a//@href").get()
        if pagination:
            yield Request(response.urljoin(pagination), callback=self.parse)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        external_id=response.url 
        if "20735700" in external_id:
            pass
        elif external_id:
            item_loader.add_value("external_id",external_id.split("au/")[-1])
            property_type = response.xpath("//h2[@class='font--tertiary mb-4']//text()").get()
            if get_p_type_string(property_type):
                item_loader.add_value("property_type", get_p_type_string(property_type))
            elif not get_p_type_string('property_type'):
                propertytype=" ".join(response.xpath("//div[@class='col-12 ']//p//text()").extract())
                if get_p_type_string(propertytype):
                    item_loader.add_value("property_type", get_p_type_string(propertytype))

            item_loader.add_value("external_link", response.url)
            
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value("city", "Mackay")
            title = response.xpath("//h2[@class='font--tertiary mb-4']//text()").get()
            if title:
                item_loader.add_value("title", title)

            dontallow=response.xpath("//h2[@class='font--tertiary mb-4']//text()").get()
            if dontallow and "warehouse" in dontallow.lower() or "Industrial" in dontallow:
                return 
            rent = response.xpath("//h4[@class='text-center text-xl-right']//text()").get()
            if rent:
                item_loader.add_value("rent", rent.split(' ')[0].split('$')[-1].strip().split('.')[0])
            item_loader.add_value("currency","AUD")
            
            room_count = response.xpath("//i[@class='fa fa-bed order-1 ']/parent::div//span//text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.strip())
            bathroom_count = response.xpath("//i[@class='fa fa-bath order-1 ']/parent::div//span//text()").get()
            if bathroom_count:
                item_loader.add_value("bathroom_count", bathroom_count.strip())
            parking = response.xpath("//i[@class='fa fa-car order-1 ']/parent::div//span//text()").get()
            if parking:
                item_loader.add_value("parking", True)
            pets_allowed = response.xpath("//i[@class='fa fa-paw order-1 ']/parent::div//span//text()").get()
            if pets_allowed:
                item_loader.add_value("pets_allowed", True)
            
            address = response.xpath("//div[@class='col-12 col-xl-6 mb-3 mb-lg-0 d-flex justify-content-center justify-content-xl-start']//h3//text()").get()
            if address:
                item_loader.add_value("address", address)
            zipcode=response.xpath("//script[contains(.,'postalCode')]/text()").get()
            if zipcode:
                zipcode1=zipcode.split("addressRegion")[-1].split(",")[0].replace('"',"").replace(":","").strip()
                zipcode=zipcode.split("postalCode")[-1].split(",")[0].replace('"',"").replace(":","").strip()
                item_loader.add_value("zipcode",zipcode+" "+zipcode1)
            desc = " ".join(response.xpath("//div[@class='col-12 ']//p//text()").extract())
            if desc:
                item_loader.add_value("description", desc.strip())
            dontallow2=" ".join(response.xpath("//div[@class='col-12 ']//p//text()").extract())
            if dontallow2 and "offices/meeting rooms" in dontallow2.lower():
                return 

            available_date = response.xpath("//p[@class='d-flex flex-row justify-content-between semi-bold']//text()").get()
            if available_date:
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
            
            dishwasher = response.xpath("//p[@class='d-flex flex-row justify-content-between semi-bold']/span[contains(.,'Dishwasher')]//text()").get()
            if dishwasher:
                item_loader.add_value("dishwasher", True)
            
            images = [x for x in response.xpath("//a[@data-fancybox='fancyboxImageGallery']//@href").getall()]
            if images:
                item_loader.add_value("images", images)
                item_loader.add_value("external_images_count", len(images))

            landlord_name = response.xpath("//div[@class='col-12 mb-2 pt-2']//h6[@class='mb-0 font-primary mb-2 bolder']/text()").get()
            if landlord_name:
                item_loader.add_value("landlord_name", landlord_name)
            landlord_phone = response.xpath("//div[@class='col-12 mb-2 pt-2']//a/span//text()").get()
            if landlord_phone:
                item_loader.add_value("landlord_phone", landlord_phone)
            landlord_email = response.xpath("//input[@name='agency[email]']/@value").get()
            if landlord_email:
                item_loader.add_value("landlord_email", landlord_email)
        yield item_loader.load_item()
        
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "flatshare" in p_type_string.lower():
        return "room"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "huis" in p_type_string.lower() or "home" in p_type_string.lower() or "cottage" in p_type_string.lower()):
        return "house"
    else:
        return None