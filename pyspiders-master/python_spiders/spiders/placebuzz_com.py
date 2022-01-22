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
from word2number import w2n
from html.parser import HTMLParser
import dateparser

class MySpider(Spider):
    name = 'placebuzz_com'
    execution_type='testing' 
    country='united_kingdom'
    locale='en' 

    def start_requests(self): 

        start_urls = [
            {
                "url" : "https://www.placebuzz.com/property-for-rent/isle-of-man/flats.isle-of-man",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.placebuzz.com/property-for-rent/isle-of-man/houses.isle-of-man",
                "property_type" : "house"
            },
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//a[contains(@class,'property-listing-list')]/@href").extract():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )
            
        
        next_page = response.xpath("//label[.='Next']/../@href").get()
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

        item_loader.add_value("external_source", "Placebuzz_PySpider_"+ self.country + "_" + self.locale)
        
        title = response.xpath("//section[@class='title-address']/h1/text()").get()
        if title:
            item_loader.add_value("title", title)
        
        city = response.xpath("//section[@class='title-address']/h2/text()").get()
        if city and city.strip() != "," and city.strip() != ".":
            item_loader.add_value("address", city)
            item_loader.add_value("city", city.split(",")[-1])

        address1= response.xpath("//p[@class='expanded']/text()").get().split(" ")[-1]
        if address1:
            item_loader.add_value("address", address1)


        desc_text = ""
        description = []
        description = response.xpath("//p[@class='expanded']/text()").getall()
        desc_html = ''      
        if description:
            desc_text = "".join(description)
            for d in description:
                desc_html += d.strip() + ' '
            desc_html = desc_html.replace('\xa0', '')
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)

        if 'sq ft' in desc_html.lower() or 'sq. ft.' in desc_html.lower() or 'sqft' in desc_html.lower():
            square_meters = desc_html.lower().split('sq ft')[0].split('sq. ft.')[0].split('sqft')[0].strip().replace('\xa0', '').split(' ')[-1]
            square_meters = str(int(float(square_meters.replace(',', '.')) * 0.09290304))
            item_loader.add_value("square_meters", square_meters)

        room_count = response.xpath("//span[@class='description' and contains(.,'bedroom')]/text()").get()
        if room_count:
            room_count = room_count.strip().replace('\xa0', '').split(' ')[0].strip()
            room_count = str(int(float(room_count)))
            item_loader.add_value("room_count", room_count)

        rent = response.xpath("//section[@class='price-mortgage']/h3/label/text()").get()
        term = response.xpath("//section[@class='price-mortgage']/h3/span/text()").get()
        if rent and term:
            rent = rent.split('£')[1].strip().replace('\xa0', '').replace(',', '')
            rent = str(int(float(rent)))
            if 'pw' in term.lower():
                rent = str(int(rent) * 4)
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", 'GBP')

        external_id = response.url.split('/')[-1]
        if external_id:
            external_id = external_id.strip()
            item_loader.add_value("external_id", external_id)

        images = [x for x in response.xpath("//meta[@property='og:image']/@content").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        

        if "no pets" in desc_text.lower():
            item_loader.add_value("pets_allowed", False)
        elif "pets" in desc_text.lower():
            item_loader.add_value("pets_allowed", True)
        
        if "garage" in desc_text.lower() or "parking" in desc_text.lower():
            item_loader.add_value("parking", True)
        
        if "deposit of " in desc_text.lower():
            deposit = desc_text.lower().split("deposit of ")[1].split(" ")[0].replace("£","")
            if deposit and deposit != "0":
                item_loader.add_value("deposit", deposit.strip())
        
        for i in description:
            if "available" in i.lower():
                try:
                    available_date = i.lower().split("available")[1].strip()
                    date_parsed = dateparser.parse(available_date)
                    like_date = date_parsed.strftime("%Y-%m")
                    item_loader.add_value("available_date", like_date)
                except:
                    pass
                break
        
        landlord_name = response.xpath("//section[contains(@class,'logo-name')]/label/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
         
        landlord_phone = response.xpath("//section[contains(@class,'phone-number')]/text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.split("on")[1].strip())
        phonee=item_loader.get_output_value("landlord_phone")
        if not phonee:
            item_loader.add_value("landlord_phone", "+44 20 3389 8445")
        
        item_loader.add_value("landlord_email", "support@placebuzz.com")

        map_data = response.xpath("//section[@class='more-properties']/a/@href").get()
        if map_data:
            item_loader.add_value("latitude", map_data.split("&Latitude=")[1].split("&")[0].strip())
            item_loader.add_value("longitude", map_data.split("&Longitude=")[1].split("&")[0].strip())
        
        
        yield item_loader.load_item()
    


class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data