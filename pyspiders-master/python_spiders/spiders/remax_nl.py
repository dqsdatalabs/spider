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
    name = 'remax_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale ='nl'
    external_source="Remax_PySpider_netherlands"
    
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.remax.nl/huurwoningen/",
                ],
            },

        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request( 
                    url=item,
                    callback=self.parse,
                )
 
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)       
        seen = False
        
        for item in response.xpath("//div[@class='object-image position-relative mb-3']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page==2 or seen:
            url = f"https://www.remax.nl/huurwoningen/?_paged={page}"
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        proptype =response.xpath("//h3[.='Bouw']/following-sibling::div//div[@class='object-feature-info text-truncate']/text()").get()

        if get_p_type_string(proptype):
            item_loader.add_value("property_type", get_p_type_string(proptype))
        else:
            return  

        item_loader.add_value("external_id", response.url.split("-")[-1].replace("/",""))

        title =" ".join(response.xpath("//h1//span[@class='object-address-line']//text()").getall())
        if title:
            title=title.replace("\n","").replace("\t","")
            title=re.sub('\s{2,}',' ',title.strip())
            item_loader.add_value("title", title)
        status=response.xpath("//div[@class='object-feature object-feature-status py-2']/div/div[2]/div/text()").get()
        if status and "verhuurd" in status.lower():
            return 
        

        address =" ".join(response.xpath("//h1//span[@class='object-address-line']//text()").getall())
        if address:
            address=address.replace("\n","").replace("\t","")
            address=re.sub('\s{2,}',' ',address.strip())
            item_loader.add_value("address", address)
            city=address.split(" ")[-1]
            item_loader.add_value("city", city)
            

        rent =response.xpath("//span[@class='object-price-value']/text()").get()
        if rent:
            rent = rent.strip().replace("\t","").replace("\n","").replace("€","").strip()
            if rent:
               item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        desc = " ".join(response.xpath("//div[@class='object-information']//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip()) 
            if desc:
                item_loader.add_value("description", desc)
        square_meters = response.xpath("//div//span[contains(text(),'Wonen')]/following-sibling::span/text()").get()
        if square_meters:
            square_meters =re.findall("\d+",square_meters)
            item_loader.add_value("square_meters", square_meters)

        room_count =response.xpath("//div[@class='object-feature object-feature-number-of-bedrooms py-2']/div/div[2]/div/text()").get()
        if room_count:
            room_count =re.findall("\d",room_count)
            item_loader.add_value("room_count", room_count)

        # bathroom_count = response.xpath("//div[contains(@class,'bath')]//text()").get()
        # if bathroom_count:
        #     bathroom_count = bathroom_count.strip().split(" ")[0]
        #     item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[@class='object-detail-photos-item mb-2']//a/img/@data-src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        from datetime import datetime
        import dateparser
        available_date = response.xpath("//h3[.='Overdracht']/following-sibling::div[2]//div[contains(text(),'Aangeboden sinds')]/parent::div/following-sibling::div/div/text()").get()
        if available_date:
            available_date = available_date.replace("\t","").replace("\n","").strip()
            if not "now" in available_date.lower():
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        parking =response.xpath("//div[@class='object-feature object-feature-parking-facility py-2']/div/div/div/text()").get()
        if parking and "soort parkeergelegenheid" in parking.lower():
            item_loader.add_value("parking", True)
            
        terrace = response.xpath("//div[@class='object-feature object-feature-garden-type py-2']/div/div/div/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        name=response.xpath("//div[@class='contact-info']/h5/a/text()").get()
        if name:
            item_loader.add_value("landlord_name",name)
        item_loader.add_value("landlord_phone", "+31228351835")
        item_loader.add_value("landlord_email", "connectbovenkarspel@remax.nl")

 

        yield item_loader.load_item()

def get_p_type_string(p_type_string): 
    if p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("hoekwoning" in p_type_string.lower() or "vrijstaande woning" in p_type_string.lower() or "tussenwoning" in p_type_string.lower()):
        return "house"
    else:
        return None