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
class MySpider(Spider):
    name = 'westsurreylettings_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'
    start_urls = ['https://westsurreylettings.co.uk/?action=epl_search&post_type=rental&property_bedrooms_max=1000']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        seen = False
        for item in response.xpath("//div[contains(@class,'property-box-right')]"):
            follow_url = response.urljoin(item.xpath(".//h3[contains(@class,'entry-title')]//@href").get())
            let_agreed = item.xpath("./div[@class='price']/span/text()[.='Let Agreed']").get()
            if let_agreed:
                continue
            prop_type = item.xpath(".//h3[contains(@class,'entry-title')]//text()").get()
            if get_p_type_string(prop_type):
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": get_p_type_string(prop_type)})
            seen = True
        
        if page == 1 or seen:
            url = f"https://westsurreylettings.co.uk/page/{page}/?action=epl_search&post_type=rental&property_bedrooms_max=1000"
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Westsurreylettings_Co_PySpider_united_kingdom")
        title = " ".join(response.xpath("//div[@class='wsltitle']//text()").getall())
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        address = ", ".join(response.xpath("//div[@class='wsltitle']//span[@class='entry-title-sub']//text()[normalize-space()]").getall())
        if address:
            item_loader.add_value("address", re.sub('\s{2,}', ' ', address.strip()))
        item_loader.add_xpath("city", "//div[@class='wsltitle']//span[@class='item-state']//text()")
        item_loader.add_xpath("zipcode", "//div[@class='wsltitle']//span[@class='item-pcode']//text()")
  
        rent_string = response.xpath("//div[@class='wsltitle']//span[@class='page-price']//text()").get()
        if rent_string: 
            item_loader.add_value("rent_string", rent_string)       
   
        description = " ".join(response.xpath("//div[@class='entry-content']//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
            
        room_count = response.xpath("//li[@class='bedrooms']/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split("bed")[0])
        bathroom_count = response.xpath("//li[@class='bathrooms']/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split("bath")[0])
      
        images = [response.urljoin(x) for x in response.xpath("//div[@class='bigimage']/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        floor = response.xpath("//li/a[contains(.,'floor')]//text()").get()
        if floor:
    
            floor = floor.split("floor")[0].strip().split(" ")[-1]
            item_loader.add_value("floor", floor)
        
        item_loader.add_value("landlord_name", "West Surrey Lettings")
        item_loader.add_value("landlord_phone", "01932 349792")
        item_loader.add_value("landlord_email", "lettings@westsurreylettings.co.uk")
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "terrace" in p_type_string.lower() or "semi detached" in p_type_string.lower()):
        return "house"
    elif p_type_string and "room" in p_type_string.lower():
        return "room"
    else:
        return None