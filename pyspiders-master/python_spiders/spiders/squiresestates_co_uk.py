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
from datetime import datetime


class MySpider(Spider):
    name = 'squiresestates_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.squiresestates.co.uk/search/?showstc=on&showsold=on&instruction_type=Letting&place=&latitude=&longitude=&bounds=&ajax_border_miles=&minpricew=&maxpricew=&property_type=Flat%2CFlat+-+above+shop%2CFlat+-+conversion%2CFlat+-+purpose+built%2CFlat+-+retirement%2CNew+Flat+-+purpose+built%2CMaisonette+-+lower%2CMaisonette+-+upper%2C&showstc%2Cshowsold=off", "property_type": "apartment"},
	        {"url": "https://www.squiresestates.co.uk/search/?showstc=on&showsold=on&instruction_type=Letting&place=&latitude=&longitude=&bounds=&ajax_border_miles=&minpricew=&maxpricew=&property_type=House%2CHouse+-+detached%2CHouse+-+end+terrace%2CHouse+-+mid+terrace%2CHouse+-+semi-detached%2CBungalow+-+semi+detached%2CBungalow+-+detached%2CBungalow+-+terrace&showstc%2Cshowsold=off", "property_type": "house"},
            {"url": "https://www.squiresestates.co.uk/search/?showstc=on&showsold=on&instruction_type=Letting&place=&latitude=&longitude=&bounds=&ajax_border_miles=&minpricew=&maxpricew=&property_type=Studio+-+conversion%2CStudio+-+purpose+built&showstc%2Cshowsold=off", "property_type": "studio"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                        })

    # 1. FOLLOWING
    def parse(self, response):
        property_type = response.meta.get("property_type")
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//div[@id='search-results']/div[contains(@class,'outer-property')]//div[@class='strong-pink-search']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type})
            seen = True
        
        if page == 2 or seen:
            base_url = response.meta.get("base_url", response.url.replace("/search/", "/search/page_count"))
            url = base_url.replace("/page_count",f"/{page}.html")
            yield Request(url, callback=self.parse, meta={"page": page+1, "base_url":base_url,"property_type":property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source","Squiresestates_Co_PySpider_"+ self.country)
        item_loader.add_value("external_link", response.url)
        title = " ".join(response.xpath("//div[@class='details-status']//text()").extract())
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        
        rent = response.xpath("//h3/strong/text()").get()
        if rent:
            price = rent.split("Per")[0].split("£")[1].strip().replace(",","")
            item_loader.add_value("rent", str(int(price)*4))
            
            address = rent.split("week")[1].strip()
            zipcode = address.split(",")[-1].strip()
            city = address.split(zipcode)[0].strip().strip(",").split(",")[-1]
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
        
        item_loader.add_value("currency", "GBP")
        
        room_count = response.xpath("//img[contains(@src,'bed')]/@alt").get()
        room_count2 = response.xpath("//img[contains(@src,'reception')]/@alt").get()
        if room_count != "0":
            item_loader.add_value("room_count", room_count)
        elif room_count2 != "0":
            item_loader.add_value("room_count", room_count2)
        elif response.meta.get("property_type") == "studio":
            item_loader.add_value("room_count", "1")
            
        bathroom_count = response.xpath("//img[contains(@src,'bath')]/@alt").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        lat_lng = response.xpath("//script[contains(.,'googlemap')]/text()").get()
        if lat_lng:
            lat = lat_lng.split("q=")[1].split("%")[0]
            lng = lat_lng.split("%2C")[1].split('"')[0]
            item_loader.add_value("latitude" , lat)
            item_loader.add_value("longitude" , lng)
        
        floor = response.xpath("//ul/li[contains(.,'Floor')]/text()").get()
        if floor and "First" in floor:
            item_loader.add_value("floor", "1")
        
        desc = "".join(response.xpath("//span[@itemprop='description']//text()").getall())
        if desc:
            item_loader.add_value("description", desc)
            
        images = [x for x in response.xpath("//div[@class='carousel-inner']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        furnished = response.xpath("//ul/li[contains(.,'Furnished')]/text()").get()
        unfurnished = response.xpath("//ul/li[contains(.,'Unfurnished')]/text()").get()
        if unfurnished:
            item_loader.add_value("furnished", False)
        if furnished:
            item_loader.add_value("furnished", True)
        
        parking = response.xpath("//ul/li[contains(.,'parking') or contains(.,'Parking')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
            
        elevator = response.xpath("//ul/li[contains(.,'lift') or contains(.,'Lift')]/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
            
        balcony = response.xpath("//ul/li[contains(.,'balcon') or contains(.,'Balcon')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//ul/li[contains(.,'terrace') or contains(.,'Terrace')]/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        available_date = response.xpath("//ul/li[contains(.,'Available')]/text()").get()
        if available_date:
            available_date = available_date.split("Available")[1].strip().replace(".","")
            if "immediately" not in available_date.lower():
                date = "{} {} ".format(available_date, datetime.now().year)
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    current_date = str(datetime.now())
                    if current_date > date2:
                        date = datetime.now().year + 1
                        parsed = date2.replace(str(date_parsed.year), str(date))
                        item_loader.add_value("available_date", parsed)
                    else:
                        item_loader.add_value("available_date", date2)  
                        
        item_loader.add_value("landlord_name", "Squires Estates")
        
        phone = response.xpath("//div[contains(@class,'details-office')]/h3/text()").get()
        if phone:
            item_loader.add_value("landlord_phone", phone)
        
        #yield item_loader.load_item()