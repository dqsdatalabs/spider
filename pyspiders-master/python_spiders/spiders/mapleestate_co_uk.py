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
from word2number import w2n

class MySpider(Spider):
    name = 'mapleestate_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source='Mapleestate_Co_PySpider_united_kingdom'
    start_urls = ["https://www.mapleestate.co.uk/Search?listingType=6&statusids=1&obc=Price&obd=Descending&category=1&areainformation=&minprice=&maxprice=&radius=&bedrooms="]
    

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page",2)

        seen = False
        for item in response.xpath("//figure/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            p_url = f"https://www.mapleestate.co.uk/Search?listingType=6&statusids=1&obc=Price&obd=Descending&category=1&areainformation=&minprice=&maxprice=&radius=&bedrooms=&page={page}"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1}
            )

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source","Mapleestate_Co_PySpider_"+ self.country)

        desc = "".join(response.xpath("//div[@class='descriptionText']/text()").getall())
        property_type = False
        if desc and "studio" in desc.lower():
            property_type = "studio"
            item_loader.add_value("property_type", "studio")
        elif desc and ("apartment" in desc.lower() or "flat" in desc.lower() or "maisonette" in desc.lower()):
            property_type = "apartment"
            item_loader.add_value("property_type", "apartment")
        elif desc and ("house" in desc.lower() or "bungalow" in desc.lower()):
            property_type = "house"
            item_loader.add_value("property_type", "house")
        else:
            return

        if desc and "SHORT TERM" in desc:
            return
        elif desc:
            desc = desc.strip().replace("\n","").replace("\t","")
            item_loader.add_value("description", desc)
        
        title = response.xpath("//title//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        address = response.xpath("//script[@id='movetoFDTitle']/text()").get()
        if address:
            address = address.replace("<h3>","").replace("</h3>","").strip()
            zipcode = address.split(",")[-1].strip()
            city = address.split(zipcode)[0].strip().strip(",").split(",")[-1].strip()
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
        
        desc = desc.lower()
        if property_type and property_type == "studio":
            item_loader.add_value("room_count", "1")
        elif "bedr" in desc.lower():
            room_count = desc.split("bedr")[0].strip().split(" ")[-1].replace("-","")
            if room_count.isdigit():
                item_loader.add_value("room_count", room_count)
            else:
                if "double" in room_count.lower():
                    room_count = desc.lower().split("double")[0].strip().split(" ")[-1].replace("-","")
                try:
                    room_count = w2n.word_to_num(room_count.replace('LET"',''))
                    item_loader.add_value("room_count", room_count)
                except:
                    pass
        else:
            room_count = desc.split("room")[0].strip().split(" ")[-1].replace("-","")
            if "double" in room_count:
                item_loader.add_value("room_count", "1")
                    
        if "bathr" in desc.lower():
            bathroom_count = desc.split("bathr")[0].strip().split(" ")[-1].replace("reception","").replace("bedroom","")
            if bathroom_count.isdigit():
                item_loader.add_value("bathroom_count", bathroom_count)
            else:           
                try:
                    bathroom_count = w2n.word_to_num(bathroom_count)
                    item_loader.add_value("bathroom_count", bathroom_count)                    
                except: pass
        
        if "floor" in desc:
            floor = desc.split("floor")[0].strip().split(" ")[-1].replace("station","").replace("furnished","").replace("kitchen","")
            if floor.replace("th","").replace("nd","").isdigit():
                item_loader.add_value("floor", floor.replace("th","").replace("nd",""))
            elif "first" in floor:
                item_loader.add_value("floor","1")
            elif "second" in floor:
                item_loader.add_value("floor","2")
            elif "third" in floor:
                item_loader.add_value("floor","3")                
                
        rent = response.xpath("//h4/div/text()").get()
        if rent:
            item_loader.add_value("rent", rent.strip().replace("£","").replace(",",""))
        item_loader.add_value("currency", "GBP")
        
        images = [ x for x in response.xpath("//div[@class='owl-carousel']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_name", "Maple Estate")
        item_loader.add_value("landlord_phone", "02084279772")
        item_loader.add_value("landlord_email","admin@mapleestate.co.uk")

        external_id = response.url.split('/')[-1]
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        from datetime import datetime
        from datetime import date
        import dateparser
        available_date = response.xpath("//text()[contains(.,'AVAILABLE FROM')]").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split('AVAILABLE FROM')[1].split('-')[0].strip().strip('MID'), date_formats=["%d/%m/%Y"], languages=['en'])
            today = datetime.combine(date.today(), datetime.min.time())
            if date_parsed:
                result = today > date_parsed
                if result == True:
                    date_parsed = date_parsed.replace(year = today.year + 1)
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

        map_url = "https://www.mapleestate.co.uk/Map-Property-Search-Results?references=" + external_id
        yield Request(map_url, callback=self.get_latlong, meta={"item_loader": item_loader})   
    
    def get_latlong(self, response):

        item_loader = response.meta.get("item_loader")
        data = json.loads(response.body)
        if 'items' in data:
            if len(data["items"]) > 0:
                item_loader.add_value("latitude", str(data["items"][0]["lat"]))
                item_loader.add_value("longitude", str(data["items"][0]["lng"]))

        yield item_loader.load_item()