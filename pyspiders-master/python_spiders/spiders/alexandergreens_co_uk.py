# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from urllib.parse import urljoin
import re
import dateparser
from geopy.geocoders import Nominatim
from word2number import w2n
class MySpider(Spider):
    name = 'alexandergreens_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'
    custom_settings = {
      "PROXY_TR_ON": True,
      "RETRY_HTTP_CODES": [500, 503, 504, 400, 401, 403, 405, 407, 408, 416, 456, 502, 429, 307],
      "HTTPCACHE_ENABLED": False,
      "RETRY_TIMES": 3,
      "DOWNLOAD_DELAY": 3,
      
    }
    handle_httpstatus_list = [401]
   
    def start_requests(self):

        start_urls = [
            {
                "url" : "https://alexandergreens.co.uk/properties-search/?type=apartment",
                "property_type" : "apartment"
            },
            {
                "url" : "https://alexandergreens.co.uk/properties-search/?type=flat",
                "property_type" : "house"
            },
            {
                "url" : "https://alexandergreens.co.uk/properties-search/?type=house-share",
                "property_type" : "room"
            },
            {
                "url" : "https://alexandergreens.co.uk/properties-search/?type=student-rooms",
                "property_type" : "student_apartment"
            },
            {
                "url" : "https://alexandergreens.co.uk/properties-search/?type=studio",
                "property_type" : "studio"
            },
            {
                "url" : "https://alexandergreens.co.uk/properties-search/?type=town-house",
                "property_type" : "house"
            },
        ] #LEVEL-1

        for url in start_urls:
            headers = {
                    'user-agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36', 
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8', 
                    'accept-language': 'en', 
                    'accept-encoding': 'gzip, deflate'
                    }
            
         
            yield Request(url=url.get('url'),
                                headers=headers,
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})



    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='rh_list_card__wrap']"):
            f_url = response.urljoin(item.xpath(".//h3/a/@href").get())
            for_sale = item.xpath(".//p[@class='price'][contains(.,' Sale')]//text()").get()
            if for_sale:
                continue
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )
            
        next_page = response.xpath("//a[contains(@class,'rh_pagination__next')]/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type" : response.meta.get("property_type")},
            )
        

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
     
        item_loader.add_value("external_source", "Alexandergreens_PySpider_"+ self.country + "_" + self.locale)

        title=response.xpath("//h1[@class='rh_page__title']//text()").extract_first()
        if title:
            title = re.sub("\s{2,}", " ", title)
            item_loader.add_value("title",title.strip())       
      
        external_id = response.xpath("//div[@class='rh_property__id']//p[@class='id']//text()").extract_first()
        if external_id:
            item_loader.add_value("external_id",external_id.strip())

        rent = response.xpath("//p[@class='price']//text()").extract_first()
        if rent:
            if "Weekly" in rent:
                numbers = re.findall(r'\d+(?:\.\d+)?', rent.replace(",","."))
                if numbers:
                    rent = int(numbers[0].replace(".",""))*4
                    rent = "£"+str(rent)
            item_loader.add_value("rent_string", rent)
        room = response.xpath("//div[@class='rh_property__meta prop_bedrooms']//div/span//text()").extract_first()
        if room:
            item_loader.add_value("room_count", room.strip())


        available_date=response.xpath("//div[@class='rh_content']/p[contains(.,'Available')][last()]/text()").get()
        try:
            if available_date:
                date2 =  available_date.split(" ")[-1].strip()
                date_parsed = dateparser.parse(
                    date2, date_formats=["%m-%d-%Y"]
                )
                date3 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date3)
        except:
            pass

        city = response.xpath("//nav[@class='property-breadcrumbs']/ul/li[last()]/a/text()").extract_first()
        if city:
            item_loader.add_value("city", city.strip())


        bathroom_count = "".join(response.xpath("//li[contains(.,'bathroom')]//text()").getall())
        if bathroom_count:
            bathroom_count = bathroom_count.split("bathroom")[0].strip()
            try:
                item_loader.add_value("bathroom_count", w2n.word_to_num(bathroom_count))
            except :
                pass

        desc = "".join(response.xpath("//div[@class='rh_content']//text()").extract())
        if desc:
            desc = re.sub("\s{2,}", " ", desc)
            item_loader.add_value("description", desc.strip())
                
        map_coordinate = response.xpath("//script[@id='property-google-map-js-extra']//text()").extract_first()
        if map_coordinate:
            latitude = map_coordinate.split('lat":"')[1].split('"')[0]
            longitude = map_coordinate.split('lng":"')[1].split('"')[0]
            geolocator = Nominatim(user_agent=response.url)
            try:
                location = geolocator.reverse(latitude + ', ' + longitude, timeout=None)
                if location.address:
                    address = location.address
                    if location.raw['address']['postcode']:
                        zipcode = location.raw['address']['postcode']
            except:
                address = None
                zipcode = None
            if address:
                item_loader.add_value("address", address)
                item_loader.add_value("zipcode", zipcode)
                item_loader.add_value("longitude", longitude)
                item_loader.add_value("latitude", latitude)
            else:
                item_loader.add_value("address", title.strip()) 
                zipcode = title.strip().split(" ")[-1]
                item_loader.add_value("zipcode", zipcode)
        
        images = [x for x in response.xpath("//div[contains(@class,'property-detail-slider-carousel-nav')]//li/img/@src").extract()]
        if images:
            item_loader.add_value("images", images)  

        parking = "".join(response.xpath("//li[contains(.,'parking')]//text()").getall())
        if parking:
            item_loader.add_value("parking", True)

        furnished = "".join(response.xpath("//li[contains(.,'Furnished')]//text()").getall())
        if furnished:
            item_loader.add_value("furnished", True)

        item_loader.add_xpath("landlord_phone", "//div//p[contains(@class,'office')]//a//text()")
        item_loader.add_xpath("landlord_email", "//div//p[contains(@class,'email')]//a//text()")
        item_loader.add_xpath("landlord_name", "//div//h3[@class='rh_property_agent__title']//text()")
        
        
        yield item_loader.load_item()

