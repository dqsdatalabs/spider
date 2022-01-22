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
    name = 'flatsinsouthsea_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en' 
    thousand_separator = ','
    scale_separator = '.'
    start_urls = ["https://www.flatsinsouthsea.co.uk/Search?listingType=6&statusids=1&obc=Price&obd=Descending&areainformation=&radius=&bedrooms=&minprice=&maxprice=&perpage=60"]

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='searchBtnRow']/a[contains(@class,'hexButtons')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
        
        next_page = response.xpath("//a[contains(@class,'page-link next')]/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse)
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)

        desc = "".join(response.xpath("//h2[.='Summary:']/following-sibling::text()").getall())
        if desc and ("apartment" in desc.lower() or "flat" in desc.lower() or "maisonette" in desc.lower()):
            item_loader.add_value("property_type", "apartment")
        elif desc and "house" in desc.lower():
             item_loader.add_value("property_type", "house")
        elif desc and "studio" in desc.lower():
             item_loader.add_value("property_type", "studio")
        elif desc and "student" in desc.lower():
             item_loader.add_value("property_type", "student_apartment")
        else:
            return
        externalid=response.url
        if externalid:
            item_loader.add_value("external_id",externalid.split("/")[-1])

        item_loader.add_value("external_source", "Flatsinsouthsea_Co_PySpider_united_kingdom")
        title = response.xpath("//div/h3/text()").extract_first()
        if title:
            item_loader.add_value("title", title.strip())
            item_loader.add_value("address", title.strip())
            item_loader.add_value("zipcode", title.split(",")[-1].strip())
            item_loader.add_value("city", title.split(",")[-2].strip())
    
        room_count = response.xpath("//div[@class='fdRooms']/span[contains(.,'bedroom')]//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split("bedroom")[0])

        bathroom_count=response.xpath("//span[contains(.,'bathroom')]//text()").get()
        if bathroom_count:
            bathroom_count = re.findall("\d+",bathroom_count)
            item_loader.add_value("bathroom_count", bathroom_count)

        rent = response.xpath("//div[@class='fdPrice']/div/text()[normalize-space()]").extract_first()
        if rent:
            item_loader.add_value("rent_string", rent)    
        available_date = response.xpath("//article/h2[contains(.,'Detail')]/following-sibling::text()[normalize-space()][contains(.,'Available')]").get()
        if available_date:
            try:
                date_parsed = dateparser.parse(available_date.split("Available")[1].strip(), languages=['en'])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
            except:
                pass 
        
        terrace = response.xpath("//ul[@class='keyFeat']/li//text()[contains(.,'terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
       
        desc = " ".join(response.xpath("//article/h2[contains(.,'Detail') and not(contains(.,'Viewing'))]/following-sibling::text()[normalize-space()]").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        images = [x for x in response.xpath("//div[@id='property-photos-device1']/a/@href").extract()]
        if images:
            item_loader.add_value("images", images)      

        item_loader.add_value("landlord_phone", "023 9283 1277")
        item_loader.add_value("landlord_email", "lettings@flatsinsouthsea.co.uk")
        item_loader.add_value("landlord_name", "Flats In Southsea")        

        yield item_loader.load_item()
