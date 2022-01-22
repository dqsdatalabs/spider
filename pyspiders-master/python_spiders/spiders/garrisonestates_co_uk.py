# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request, FormRequest
from python_spiders.loaders import ListingItem
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re

class MySpider(Spider):
    name = "garrisonestates_co_uk"
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'
    page_num = 1
    def start_requests(self):
        formdata = {
            "sortorder": "price-desc",
            "RPP": "12",
            "OrganisationId": "ccafe436-5ae7-4678-8246-550164eac3fd",
            "WebdadiSubTypeName": "Rentals",
            "Status": "{2a50fde6-8f09-4d01-9514-7a856e206d04},{e9617465-c405-4b6a-abc9-fdbfc499145c}",
            "includeSoldButton": "false",
            "page": str(self.page_num),
        }
        url = "https://www.garrisonestates.co.uk/api/set/results/grid"
        yield FormRequest(
            url=url,
            callback=self.parse,
            formdata=formdata)            

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='property '][div[contains(.,'To Let')]]//div[@class='property-description']/a/@href").extract():
            yield Request(response.urljoin(item), self.populate_item)

        next_page = response.xpath("//li/a[.='»']/@data-page").extract_first()
        if next_page:
            self.page_num +=1
            formdata = {
            "sortorder": "price-desc",
            "RPP": "12",
            "OrganisationId": "ccafe436-5ae7-4678-8246-550164eac3fd",
            "WebdadiSubTypeName": "Rentals",
            "Status": "{2a50fde6-8f09-4d01-9514-7a856e206d04},{e9617465-c405-4b6a-abc9-fdbfc499145c}",
            "includeSoldButton": "false",
            "page": str(self.page_num),
            }
            url = "https://www.garrisonestates.co.uk/api/set/results/grid"
            yield FormRequest(
                url=url,
                callback=self.parse,
                formdata=formdata)     
    # 2. SCRAPING LEVEL 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/property/")[1].split("/")[0])
        property_type = " ".join(response.xpath("//title/text()").getall()).strip()
        if get_p_type_string(property_type): 
            item_loader.add_value("property_type", get_p_type_string(property_type))
        else: 
            return
        item_loader.add_value("external_source", "Garrisonestates_Co_PySpider_united_kingdom")
        title = response.xpath("//section[@id='description']//h2/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        cords = response.xpath("//section[@id='maps']/@data-cords").get()
        if cords:
            item_loader.add_value("latitude", cords.split('"lat": "')[1].split('"')[0].strip())
            item_loader.add_value("longitude", cords.split('"lng": "')[1].split('"')[0].strip())
    
        rent = "".join(response.xpath("//div[contains(@class,' property-price')]//text()[normalize-space()]").getall())
        if rent:
            if "week" in rent:                                    
                rent = re.search(r"\d+,*\d+", rent).group() #"".join(filter(str.isnumeric, rent.replace(',', '').replace('\xa0', '')))
                item_loader.add_value("rent", str(int(float(rent.replace(",", ""))*4)))
                item_loader.add_value("currency", "GBP")
            else:                                    
                item_loader.add_value("rent_string", rent) 
        
        description = "".join(response.xpath("//section[@id='description']/p//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        address = ", ".join(response.xpath("//div[@class='col-sm-5 property-address']/h1//text()[normalize-space()]").getall())
        if address:  
            address = re.sub("\s{2,}", " ", address.strip())
            item_loader.add_value("address", address.replace(", ,",","))
        zipcode = response.xpath("//div[@class='col-sm-5 property-address']/h1/span[@class='displayPostCode']/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())
        city = response.xpath("//div[@class='col-sm-5 property-address']/h1/span[@class='city']/text()").get()
        if city:
            item_loader.add_value("city", city.replace(",","").strip())
        room_count = response.xpath("//aside[@id='sidebar']//li[img[contains(@src,'bedrooms.svg')]]/span/text()[.!='0']").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        elif get_p_type_string(property_type) == "studio":
            item_loader.add_value("room_count", "1")

        item_loader.add_xpath("bathroom_count", "//aside[@id='sidebar']//li[img[contains(@src,'bathroom')]]/span/text()")
    
        features = ", ".join(response.xpath('.//div[@id="collapseOne"]//li/text()').extract())
        if "parking" in features.lower():
            item_loader.add_value('parking', True)
        
        if "terrace" in features.lower():
            item_loader.add_value('terrace', True)

        if "swimming pool" in features.lower():
            item_loader.add_value('swimming_pool', True)

        if "elevator" in features.lower() or "lift" in features.lower():
            item_loader.add_value('elevator', True)
        
        if "balcony" in features.lower():
            item_loader.add_value('balcony', True)

        if "dishwasher" in features.lower():
            item_loader.add_value('dishwasher', True)
        furnished = response.xpath("//span[@class='furnished']/text()").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
    
        images = [response.urljoin(x) for x in response.xpath("//div[contains(@class,'owl-image lazyload')]/@data-bg").getall()]
        if images:
            item_loader.add_value("images", images)
            
        floor_plan_images = [response.urljoin(x) for x in response.xpath("//h2/../..//img[@title='floorplan']/@data-src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        item_loader.add_value("landlord_name", "Garrison Estates")
        item_loader.add_xpath('landlord_phone', '//a[contains(@href,"tel:")]/text()')
        item_loader.add_xpath('landlord_email', '//a[contains(@href,"mailto:")]/text()')
        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "duplex" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "terrace" in p_type_string.lower() or "detached" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "house"
    else:
        return None
    
