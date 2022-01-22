# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from html.parser import HTMLParser
import re 

class MySpider(Spider):
    name = 'century21uk_com' 
    execution_type='testing'
    country='united_kingdom'
    locale='en' # LEVEL 1

    custom_settings = {
        "CONCURRENT_REQUESTS" : 2,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 1,
        "PROXY_ON" : True
    }

    def start_requests(self):
        start_urls = [
            {"url": "https://www.century21uk.com/property-search/?location=&lat=&lng=&type=flat&min=0&rooms=0&radius=5.1&added=any&max=0&maxrooms=0&agentID=&ownership=lettings", "property_type": "apartment"},
	        # {"url": "https://www.century21uk.com/property-search/?location=&lat=&lng=&type=house&min=0&rooms=0&radius=5.1&added=any&max=0&maxrooms=0&agentID=&ownership=lettings", "property_type": "house"},
            # {"url": "https://www.century21uk.com/property-search/?location=&lat=&lng=&type=bungalow&min=0&rooms=0&radius=5.1&added=any&max=0&maxrooms=0&agentID=&ownership=lettings", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            })
    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='property-tile']/div/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
        next_page = response.xpath("//a[contains(@class,'next')]/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')}
            )
 
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get("property_type"))
        item_loader.add_value("external_link", response.url)
      
        
        item_loader.add_value("external_source", "Century21uk_PySpider_"+ self.country + "_" + self.locale)

        title = response.xpath("//title[1]/text()").get()
        if title:
            item_loader.add_value("title", title.strip()) 

        address = response.xpath("//title[1]/text()").get()
        if address:
            address= address.split("|")[0].strip()
            city = address.split(",")[-2].strip()
            zipcode = address.split(",")[-1].strip()
            item_loader.add_value("address",address)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
        
        bathroom_count = response.xpath("//i[contains(@class,'shower-icon')]/../text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.lower().split('bath')[0].strip())

        parking = response.xpath("//strong[.='Parking']").get()
        if parking:
            item_loader.add_value("parking", True)
        
        latitude = response.xpath("//div[@id='map']/@data-lat").get()
        longitude = response.xpath("//div[@id='map']/@data-lng").get()
        if latitude and longitude:
            latitude = latitude.strip()
            longitude = longitude.strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        square_meters = response.xpath("//div[contains(@class,'description')]//text()").getall()
        if square_meters:
            sqm = ''
            for text in square_meters:
                if 'sq ft' in text.lower():
                    sqm = text.lower().split('sq ft')[0].strip().split(' ')[-1].strip().replace(',', '').replace('.', '')
                    break
            if sqm != '':
                sqm = str(int(float(sqm) * 0.09290304))
                item_loader.add_value("square_meters", sqm)
                
        room_count = response.xpath("//li[contains(.,'Bedroom')]/text()").get()
        if room_count:
            room_count = room_count.split(':')[-1].strip().replace('\xa0', '').split(' ')[0].strip()
            room_count = str(int(float(room_count)))
            item_loader.add_value("room_count", room_count)

        rent = response.xpath("//small[contains(.,'PCM')]/parent::b/text()").get()
        if rent:
            rent = rent.split('£')[1].strip().replace('\xa0', '')
            rent = rent.replace(',', '').replace('.', '')
            rent = rent.split(' ')
            reg_rent = []
            for i in rent:
                if i.isnumeric():
                    reg_rent.append(i) 
            r = "".join(reg_rent)
            item_loader.add_value("rent", r)
            item_loader.add_value("currency", 'GBP')

        external_id = response.url.split('/')[-3].strip()
        if external_id:
            external_id = external_id.strip()
            item_loader.add_value("external_id", external_id)

        description = response.xpath("//div[contains(@class,'description')]//p//text()").getall()
        desc_html = ''       
        if description:
            for d in description:
                desc_html += d.strip() + ' '
            desc_html = desc_html.replace('\xa0', '')
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)
        
        available_date ="".join(response.xpath("//div[contains(@class,'description')]//p//text()").getall())
        if available_date:
            available=available_date.replace("/"," ")
            available=re.search("(\d+)\s(\d+)\s(\d+)",available)
            if available:
                date=available.group() 
                date=date.replace(" ","-")
                item_loader.add_value("available_date", date)
        
        furnished ="".join(response.xpath("//div[contains(@class,'description')]//p//text()").getall())
        if furnished:
            furn=re.findall("FURNISHED",furnished)
            if furn:
                unfurn=re.findall("UNFURNISHED",furnished)
                if unfurn:
                    item_loader.add_value("furnished", False)
                else:
                    item_loader.add_value("furnished", True)

            #    item_loader.add_value("furnished", True)




        images = [x for x in response.xpath("//ul[@id='property-images']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))

        floor_plan_images = response.xpath("//div[contains(@class,'floorplan')]/img/@src").get()
        if floor_plan_images: 
            floor_plan_images = floor_plan_images.strip()
            item_loader.add_value("floor_plan_images", floor_plan_images)

        
     
            
        item_loader.add_value("landlord_name", "CENTURY 21")

        landlord_phone =response.xpath("//p[@class='number']/text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
            item_loader.add_value("landlord_phone", landlord_phone)
        elif not landlord_phone:
            landlord_phone =response.xpath("//a[contains(@href,'tel')]/@href").get()
            item_loader.add_value("landlord_phone", landlord_phone.split("tel:")[1])


        landlord_email = response.xpath("//div[@class='foot-contact']/ul/li[3]/a/text()").get()
        if landlord_email:
            landlord_email = landlord_email.strip()
            item_loader.add_value("landlord_email", landlord_email)
        
        yield item_loader.load_item()

class HTMLFilter(HTMLParser): 
    text = ''
    def handle_data(self, data):
        self.text += data
