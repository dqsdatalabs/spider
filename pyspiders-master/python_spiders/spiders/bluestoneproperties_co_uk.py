# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json, re
import dateparser

class MySpider(Spider):
    name = 'bluestoneproperties_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source='Bluestoneproperties_Co_PySpider_united_kingdom'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.bluestoneproperties.co.uk/Search?listingType=6&statusids=1&obc=Price&obd=Descending&areainformation=&radius=&bedrooms=&minprice=&maxprice=&dbsids=7&page=1", "property_type": "apartment"},
            {"url": "https://www.bluestoneproperties.co.uk/Search?listingType=6&statusids=1&obc=Price&obd=Descending&areainformation=&radius=&bedrooms=&minprice=&maxprice=&dbsids=4&page=1", "property_type": "house"},
	        {"url": "https://www.bluestoneproperties.co.uk/Search?listingType=6&statusids=1&obc=Price&obd=Descending&areainformation=&radius=&bedrooms=&minprice=&maxprice=&dbsids=8&page=1", "property_type": "house"},
            {"url": "https://www.bluestoneproperties.co.uk/Search?listingType=6&statusids=1&obc=Price&obd=Descending&areainformation=&radius=&bedrooms=&minprice=&maxprice=&dbsids=10&page=1", "property_type": "house"},
            {"url": "https://www.bluestoneproperties.co.uk/Search?listingType=6&statusids=1&obc=Price&obd=Descending&areainformation=&radius=&bedrooms=&minprice=&maxprice=&dbsids=12&page=1", "property_type": "studio"},  
            {"url": "https://www.bluestoneproperties.co.uk/Search?listingType=6&statusids=1&obc=Price&obd=Descending&areainformation=&radius=&bedrooms=&minprice=&maxprice=&dbsids=36&page=1", "property_type": "room"}    
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                        })

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page',2)
        seen = False
        for url in response.xpath("//a[contains(.,'Full')]/@href").extract():
            yield Request(response.urljoin(url), callback=self.populate_item, meta={"property_type": response.meta.get("property_type")})
            seen = True

        if page==2 or seen:
            url = response.url.replace(f"page={page-1}", f"page={page}")
            yield Request(url, callback=self.parse, meta={"property_type":response.meta.get("property_type")})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))

        title = response.xpath("//h3//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Bluestoneproperties_Co_PySpider_"+ self.country)
       
        address = response.xpath("//h3/text()").extract_first()     
        if address:   
            item_loader.add_value("address",address.strip())
            city = address.split(", ")[-2].strip()
            zipcode = address.split(", ")[-1].strip().replace("United Kingdom","")
            item_loader.add_value("city",city)
            item_loader.add_value("zipcode",zipcode.strip())
     
        # external_id = response.xpath("//div[@class='specs']/div[@class='ref']/text()").extract_first()     
        # if external_id:   
        #     item_loader.add_value("external_id",external_id.split(":")[1].strip())

        rent =" ".join(response.xpath("//div[@class='fdPrice']/div/text()").extract())
        if rent:
            item_loader.add_value("rent_string", rent.replace(",",".").strip().split(" ")[0])   

        rooms = response.xpath("//div[@class='fdRooms']/span/text()").extract()

        if "studio" in response.meta.get('property_type'):
            item_loader.add_value("room_count", "1")
        elif "room" in response.meta.get('property_type'):
            item_loader.add_value("room_count", "1")
        elif rooms:
            for i in rooms:
                if "bed" in i.lower():
                    if i.split(" ")[0] !='0':
                        item_loader.add_value("room_count", i.split(" ")[0])
                if "bath" in i.lower():
                    item_loader.add_value("bathroom_count", i.split(" ")[0])
        
        available_date = response.xpath("//div[@class='item']/div[contains(.,'Available From')]/following-sibling::div//text()").extract_first()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d %B %Y"], languages=['en'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

        desc = " ".join(response.xpath("//h2[contains(.,'Summary')]/following-sibling::text()[1]").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
        
        if not item_loader.get_collected_values("description"):
            desc = "".join(response.xpath("//h2[contains(.,'Full Detail')]/following-sibling::text()[1]").getall())
            if desc:
                item_loader.add_value("description", desc.strip())

        images = [x for x in response.xpath("//div[contains(@class,'royalSlider')]//@src").extract()]
        if images:
            item_loader.add_value("images", images)      
      
        item_loader.add_value("landlord_phone", "0208 355 3405")
        item_loader.add_value("landlord_email", "info@bluestoneproperties.co.uk")
        item_loader.add_value("landlord_name", "Bluestone Properties")

        yield item_loader.load_item()