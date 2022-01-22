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
    name = 'letbid_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.letbid.com/properties?p=1&premium_property=2&loc=&lat=&lng=&rad=&min_price=&max_price=&filter_attribute[numeric][2][min]=&filter_attribute[numeric][2][max]=&filter_attribute[numeric][3][min]=&filter_attribute[numeric][3][max]=&filter_attribute[numeric][4][min]=&filter_attribute[numeric][4][max]=&price_sort=&filter_attribute[categorical][1]=Flat&filter_attribute[categorical][30]=&filter_attribute[categorical][31]=&loc=&lat=&lng=&rad=&price_sort=", "property_type": "apartment"},
            {"url": "https://www.letbid.com/properties?p=1&premium_property=2&loc=&lat=&lng=&rad=&min_price=&max_price=&filter_attribute[numeric][2][min]=&filter_attribute[numeric][2][max]=&filter_attribute[numeric][3][min]=&filter_attribute[numeric][3][max]=&filter_attribute[numeric][4][min]=&filter_attribute[numeric][4][max]=&price_sort=&filter_attribute[categorical][1]=Studio&filter_attribute[categorical][30]=&filter_attribute[categorical][31]=&loc=&lat=&lng=&rad=&price_sort=", "property_type": "studio"},
            {"url": "https://www.letbid.com/properties?p=1&premium_property=2&loc=&lat=&lng=&rad=&min_price=&max_price=&filter_attribute[numeric][2][min]=&filter_attribute[numeric][2][max]=&filter_attribute[numeric][3][min]=&filter_attribute[numeric][3][max]=&filter_attribute[numeric][4][min]=&filter_attribute[numeric][4][max]=&price_sort=&filter_attribute[categorical][1]=Block+Of+Flats&filter_attribute[categorical][30]=&filter_attribute[categorical][31]=&loc=&lat=&lng=&rad=&price_sort=", "property_type": "apartment"},
            {"url": "https://www.letbid.com/residential-lettings", "property_type": "house"},
	        {"url": "https://www.letbid.com/properties?p=1&premium_property=2&loc=&lat=&lng=&rad=&min_price=&max_price=&filter_attribute[numeric][2][min]=&filter_attribute[numeric][2][max]=&filter_attribute[numeric][3][min]=&filter_attribute[numeric][3][max]=&filter_attribute[numeric][4][min]=&filter_attribute[numeric][4][max]=&price_sort=&filter_attribute[categorical][1]=House&filter_attribute[categorical][30]=&filter_attribute[categorical][31]=&loc=&lat=&lng=&rad=&price_sort=", "property_type": "house"},
            {"url": "https://www.letbid.com/properties?p=1&premium_property=2&loc=&lat=&lng=&rad=&min_price=&max_price=&filter_attribute[numeric][2][min]=&filter_attribute[numeric][2][max]=&filter_attribute[numeric][3][min]=&filter_attribute[numeric][3][max]=&filter_attribute[numeric][4][min]=&filter_attribute[numeric][4][max]=&price_sort=&filter_attribute[categorical][1]=Semi+Detached&filter_attribute[categorical][30]=&filter_attribute[categorical][31]=&loc=&lat=&lng=&rad=&price_sort=", "property_type": "house"},
            {"url": "https://www.letbid.com/properties?p=1&premium_property=2&loc=&lat=&lng=&rad=&min_price=&max_price=&filter_attribute[numeric][2][min]=&filter_attribute[numeric][2][max]=&filter_attribute[numeric][3][min]=&filter_attribute[numeric][3][max]=&filter_attribute[numeric][4][min]=&filter_attribute[numeric][4][max]=&price_sort=&filter_attribute[categorical][1]=Semi+Detached+Bungalow&filter_attribute[categorical][30]=&filter_attribute[categorical][31]=&loc=&lat=&lng=&rad=&price_sort=", "property_type": "house"},   
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
        for item in response.xpath("//div[@id='main-content']/div[contains(@class,'feature_property_list')]/div/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type})
            seen = True
        
        if page == 2 or seen:
            url = f"https://www.letbid.com/residential-lettings?p={page}&premium_property=2"
            # url = base_url.replace("?p=1", f"?p={page}")
            yield Request(url, callback=self.parse, meta={"page": page+1, "property_type":property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title", "normalize-space(//h1/text())")
        item_loader.add_value("external_source","Letbid_PySpider_united_kingdom")
   
        address = response.xpath("//div[@class='estate-explore-location']/text()").extract_first()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("zipcode",address.split(",")[-1].strip())
            item_loader.add_value("city",address.split(",")[-2].strip())

        ext_id = response.xpath("//ul[@class='list-info']/li[span[contains(.,'Reference number')]]/text()").extract_first()     
        if ext_id:   
            item_loader.add_value("external_id",ext_id.strip())

        available_date = response.xpath("//ul[@class='list-info']/li[span[contains(.,'Available from')]]/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.strip(), languages=['en'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
  
        rent = response.xpath("//div[@class='property-detail-price']//text()").extract_first()
        if rent:
            item_loader.add_value("rent_string", rent.replace(",","."))   
            
        desc = " ".join(response.xpath("//div[@id='full_notice_description']//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
        deposit = " ".join(response.xpath("//div[@id='full_notice_description']/p//text()[contains(.,'Deposit')]").extract())
        if deposit:
            deposit = deposit.split("£")[1].replace(",",".")       
            if "(" in deposit:
                deposit = deposit.split("(")[0]
            dep = deposit.strip().split(" ")[0].strip()
            item_loader.add_value("deposit", int(float(dep.strip())))
  
        room_count = response.xpath("//li[contains(.,'Bedroom')]/span[@class='label-content']/text()").get()
        if room_count and room_count.strip() !="0":
            item_loader.add_value("room_count", room_count)


        bathroom_count = response.xpath("//li[contains(.,'Bathroom')]/span[@class='label-content']/text()").get()
        if bathroom_count and bathroom_count.strip() !="0":
            item_loader.add_value("bathroom_count", bathroom_count)

       
        images = [x for x in response.xpath("//div[@id='slider']/ul/li/img/@src").extract()]
        if images:
            item_loader.add_value("images", images)   

        floor = response.xpath("//ul[@class='ex-fea-list']/li[contains(.,'Floor')]//text()").get()
        if floor:
            item_loader.add_value("floor", floor.split("Floor")[0].strip()) 
            
        map_coordinate = response.xpath("//script[@type='text/javascript']/text()[contains(.,'showMap(') and contains(.,'function defaultLocation()')]").extract_first()
        if map_coordinate:
            map_coordinate = map_coordinate.split('function defaultLocation()')[1]
            latitude = map_coordinate.split('showMap(')[1].split(',')[0].strip()
            longitude = map_coordinate.split(',')[1].split(');')[0].strip()
            if latitude and longitude:
                item_loader.add_value("longitude", longitude)
                item_loader.add_value("latitude", latitude)

        parking = response.xpath("//ul[@class='list-info']/li[span[contains(.,'parking') or contains(.,'Parking') ]]/text()").get()
        if parking:
            item_loader.add_value("parking", True) 
        elif not parking:
            parking = response.xpath("//div[@id='full_notice_description']/p//text()[contains(.,'Parking')][not(contains(.,'None'))]").extract_first()
            if parking:
                item_loader.add_value("parking", True) 

        elevator = response.xpath("//ul[@class='list-info']/li[span[contains(.,'Elevator')]]/text()").get()
        if elevator:
            item_loader.add_value("elevator", True) 
            
        balcony = response.xpath("//ul[@class='ex-fea-list']/li[contains(.,'Balcony')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True) 

        swimming_pool = response.xpath("//ul[@class='list-info']/li[span[contains(.,'Pool')]]/text()").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True) 
        
        washing_machine = response.xpath("//ul[@class='list-info']/li[span[contains(.,'Washing Machine')]]/text()").get()
        if washing_machine:
            if "no" in washing_machine.lower():
                item_loader.add_value("washing_machine", False) 
            else:
                item_loader.add_value("washing_machine", True) 

        furnished = response.xpath("//ul[@class='list-info']/li[span[contains(.,'Furnishing')]]/text()").get()
        if furnished:
            if "Furnished or Unfurnished" in furnished:
                pass
            elif "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False) 
            else:
                item_loader.add_value("furnished", True) 
        item_loader.add_value("landlord_phone", "0161 222 9650")
        item_loader.add_value("landlord_email", "info@letbid.com")
        item_loader.add_value("landlord_name", "LetBid")


        yield item_loader.load_item()