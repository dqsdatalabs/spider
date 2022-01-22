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
import dateparser

class MySpider(Spider): 
    name = 'leonardleese_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en' 
    external_source="Leonardleese_PySpider_united_kingdom_en"

    def start_requests(self):

        start_urls = [
            {
                "url" : "https://www.leonardleese.com/property/?wppf_search=to-rent&wppf_property_type=apartment&wppf_radius=10&wppf_orderby=latest&wppf_view=list&wppf_lat=0&wppf_lng=0",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.leonardleese.com/property/?wppf_search=to-rent&wppf_property_type=flat&wppf_radius=10&wppf_view=grid&wppf_lat=0&wppf_lng=0",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.leonardleese.com/property/?wppf_search=to-rent&wppf_property_type=terraced&wppf_radius=10&wppf_view=grid&wppf_lat=0&wppf_lng=0",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.leonardleese.com/property/?wppf_search=to-rent&wppf_property_type=penthouse&wppf_radius=10&wppf_view=list&wppf_lat=0&wppf_lng=0",
                "property_type" : "house"
            },
            {
                "url" : "https://www.leonardleese.com/property/?wppf_search=to-rent&wppf_property_type=town-house&wppf_radius=10&wppf_orderby=latest&wppf_view=list&wppf_lat=0&wppf_lng=0",
                "property_type" : "house"
            },
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//h4/a/@href").extract():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )
       
        next_page = response.xpath("//a[contains(@class,'next')]/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type" : response.meta.get("property_type")},
            ) 
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        rented = response.xpath("//h1/span[@class='bolden']/text()[contains(.,'Let Agreed')]").extract_first()
        if rented:
            return
        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", self.external_source)

        latitude_longitude = response.xpath("//script[contains(.,'LatLng')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0].strip()
            longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)

        description = response.xpath("//h3[contains(.,'About the Property')]/following-sibling::div/p/text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d.strip() + ' '
            desc_html = desc_html.replace('\xa0', '')
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)

        if 'sq ft' in desc_html.lower() or 'sq. ft.' in desc_html.lower() or 'sqft' in desc_html.lower():
            square_meters = desc_html.lower().split('sq ft')[0].split('sq. ft.')[0].split('sqft')[0].strip().replace('\xa0', '').split(' ')[-1]
            if "approximately" in square_meters:
                square_meters = desc_html.lower().split('sqft')[1].strip().replace('\xa0', '').split(' ')[0]
            square_meters = str(int(float(square_meters.replace(',', '.').strip('+')) * 0.09290304))
            item_loader.add_value("square_meters", square_meters)

        room_count = response.xpath("//strong[contains(.,'Bedroom')]/following-sibling::text()").get()
        if room_count:
            room_count = room_count.strip().replace('\xa0', '').split(' ')[0].strip()
            room_count = str(int(float(room_count)))
            item_loader.add_value("room_count", room_count)

        bathroom_count= "".join(response.xpath("//strong[contains(.,'Bathrooms')]/following-sibling::text()").getall())
        if room_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_xpath("title", "//title/text()")


        address = "".join(response.xpath("//div[@class='wppf_property_keyfeatures wppf_pad']/div[strong='Location:']/text()").extract())
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("zipcode", address.strip().split(",")[-1].strip())
            item_loader.add_value("city", address.strip().split(",")[0].strip())

        rent = response.xpath("//span[@class='up-price']/text()").get()
        if rent:
            rent = rent.split('£')[1].strip().replace('\xa0', '').replace(' ', '').replace(',', '')
            if 'pw' in rent:
                rent = str(int(rent.strip('pw').split('\n')[0]) * 4)
            if 'pcm' in rent:
                rent = rent.strip('pcm').split('\n')[0]
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", 'GBP')

        images = [x for x in response.xpath("//div[@id='wppf_slideshow_nav']//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        floor_plan_images = [x for x in response.xpath("//a[contains(@href,'floorplan')]/img/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        available_date = response.xpath("//div[@class='wppf_property_about wppf_pad']//ul//li[contains(.,'Available')]/text()").get()
        if available_date:
            
                available_date = available_date.strip().split(':')[-1].strip()
                if available_date.isalpha() != True:
                    date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                    if date_parsed:
                        date2 = date_parsed.strftime("%Y-%m-%d")
                        item_loader.add_value("available_date", date2)

        furnished = response.xpath("//li[contains(.,'Furnished')]  ").get()
        if furnished:
            furnished = True
            item_loader.add_value("furnished", furnished)
        else:

            furnished = response.xpath("//div[@class='wppf_property_about wppf_pad']/ul/li[.='Furnished']/text()").get()
            if furnished:
                furnished = True
                item_loader.add_value("furnished", True)

        # furnished = response.xpath("//li[contains(.,'Furnished')]  ").get()
        # if furnished:
        #     furnished = True
        #     item_loader.add_value("furnished", furnished)

        floor = response.xpath("//li[contains(text(),'Floor')]").get()
        if floor:
            floor = floor.strip().split(' ')[0]
            item_loader.add_value("floor", floor)

        balcony = response.xpath("//li[contains(.,'Balcony')]").get()
        if balcony:
            balcony = True
            item_loader.add_value("balcony", balcony)

        terrace = response.xpath("//li[contains(.,'Terrace')]").get()
        if terrace:
            terrace = True
            item_loader.add_value("terrace", terrace)

        landlord_phone = response.xpath("//text()[contains(.,'Tel')]/following-sibling::strong[1]/text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
            item_loader.add_value("landlord_phone", landlord_phone)

        landlord_email = response.xpath("//text()[contains(.,'Tel')]/following-sibling::strong[2]/text()").get()
        if landlord_email:
            landlord_email = landlord_email.strip()
            item_loader.add_value("landlord_email", landlord_email)
            item_loader.add_value("landlord_name", "Leonard Leese")
     
        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data