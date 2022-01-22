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
from html.parser import HTMLParser
import re

class MySpider(Spider):
    name = 'drivers_co_uk' 
    execution_type='testing'
    country='united_kingdom'
    locale='en' # LEVEL 1

    def start_requests(self):
        start_urls = [
            {"url": "https://www.drivers.co.uk/search/1.html?instruction_type=Letting&showsold=on&department=%21Commercial&xml_type=reapit_webservice&address_keyword=&minprice=&maxprice=&property_type=Flat%2FApartment&showstc=on",
             "property_type": "apartment"},
            {"url": "https://www.drivers.co.uk/search/1.html?instruction_type=Letting&showsold=on&department=%21Commercial&xml_type=reapit_webservice&address_keyword=&minprice=&maxprice=&property_type=Maisonette&showstc=on",
            "property_type": "house"},
            {"url": "https://www.drivers.co.uk/search/1.html?instruction_type=Letting&showsold=on&department=%21Commercial&xml_type=reapit_webservice&address_keyword=&minprice=&maxprice=&property_type=House&showstc=on",
             "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//div[@class='prop-contain']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get("property_type")})
            seen = True
        
        if page == 2 or seen:
            url = ''
            if 'Flat%2FApartment' in response.url:
                url = f"https://www.drivers.co.uk/search/{page}.html?instruction_type=Letting&showsold=on&department=%21Commercial&xml_type=reapit_webservice&address_keyword=&minprice=&maxprice=&property_type=Flat%2FApartment&showstc=on"
            elif 'Maisonette' in response.url:
                url = f"https://www.drivers.co.uk/search/{page}.html?instruction_type=Letting&showsold=on&department=%21Commercial&xml_type=reapit_webservice&address_keyword=&minprice=&maxprice=&property_type=Maisonette&showstc=on"
            elif 'House' in response.url:
                url = f"https://www.drivers.co.uk/search/{page}.html?instruction_type=Letting&showsold=on&department=%21Commercial&xml_type=reapit_webservice&address_keyword=&minprice=&maxprice=&property_type=House&showstc=on"
            yield Request(url, callback=self.parse, meta={"page": page+1, "property_type": response.meta.get("property_type")})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        property_type = response.meta.get("property_type")
        item_loader.add_value("property_type", property_type)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("drv-")[1].split("/")[0])

        item_loader.add_value("external_source", "Driversco_PySpider_"+ self.country + "_" + self.locale)

        title = "".join(response.xpath("//h1//span//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        latitude_longitude = response.xpath("//script[contains(.,'lng')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('&q=')[1].split('%')[0].strip()
            longitude = latitude_longitude.split('&q=')[1].split('-')[1].split('"')[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        description = response.xpath("//div[@class='col-sm-12 col-md-8']/p/text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d.strip() + ' '
            desc_html = desc_html.replace('\xa0', '')
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)

        if 'sq ft' in desc_html.lower() or 'sq. ft.' in desc_html.lower() or 'sqft' in desc_html.lower():
            square_meters = desc_html.split('sq ft')[0].split('sq. ft.')[0].split('sqft')[0].strip().replace('\xa0', '').split(' ')[-1]
            square_meters = str(int(float(square_meters.replace(',', '.')) * 0.09290304))
            item_loader.add_value("square_meters", square_meters)

        room_count = response.xpath("//img[contains(@src, 'bed')]/following-sibling::strong[1]/text()").get()
        if room_count:
            room_count = room_count.replace('\xa0', '').strip()
            room_count = str(int(float(room_count)))
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//img[contains(@src, 'bath')]/following-sibling::strong[1]/text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.replace('\xa0', '').strip()
            bathroom_count = str(int(float(bathroom_count)))
            item_loader.add_value("bathroom_count", bathroom_count)
        
        address = "".join(response.xpath("//h1[contains(@class,'h2')]/span[@itemprop='name']/text()").getall())
        if address:
            item_loader.add_value("address", address.strip())

        city = response.xpath("//input[@name='propertyAddress']/@value").get()
        if city:
            item_loader.add_value("city", city.split(",")[-1].strip())
        
        rent = response.xpath("//span[@itemprop='price']/text()").get()
        if rent:
            rent = rent.split('£')[1].split('P')[0].strip().replace('\xa0', '')
            rent = rent.replace(',', '').replace('.', '')
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", 'GBP')

        images = [urljoin('https://www.drivers.co.uk', x) for x in response.xpath("//div[@id='property-carousel']/div//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))

        floor_plan_images = [urljoin('https://www.drivers.co.uk', x) for x in response.xpath("//div[@id='floorplan']/img/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)


        item_loader.add_value("landlord_name", "Drivers & Norris")

        landlord_phone = response.xpath("//a[contains(@href, 'tel')]/@href").re_first(r'\d+')
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)

        landlord_email = response.xpath("//a[contains(@href, 'mailto')]/@href").re_first(r"mailto:(.*)")
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email)

        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data