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
from  geopy.geocoders import Nominatim
from html.parser import HTMLParser
from word2number import w2n
import dateparser

class MySpider(Spider):
    name = 'regal_estates_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en' 
    external_source="Regalestatesco_PySpider_united_kingdom_en"
    # LEVEL 1
    
    def start_requests(self):
        start_urls = [
            {
                "url" : "https://www.regal-estates.co.uk/property/?search=let&location=&price_min=0&price_max=0&price_min=0&price_max=0&beds_min=0&beds_max=0&radius=0&type=&type=house&added=&items=&sort=",
                "property_type" : "house"
            },
            {
                "url" : "https://www.regal-estates.co.uk/property/?search=let&location=&price_min=0&price_max=0&price_min=0&price_max=0&beds_min=0&beds_max=0&radius=0&type=&type=apartment&added=&items=&sort=",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.regal-estates.co.uk/property/?search=let&location=&price_min=0&price_max=0&price_min=0&price_max=0&beds_min=0&beds_max=0&radius=0&type=&type=student&added=&items=&sort=",
                "property_type" : "student_apartment"
            },
            
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING 
    def parse(self, response):
        
        for item in response.xpath("//main[@id='category-property']/article//a/@href").extract():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            ) 
        
        next_page = response.xpath("//i[contains(@class,'fa-caret-right')]/../../@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={
                    "property_type" : response.meta.get("property_type"),
                }
            )
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        commercial = response.xpath("//main[@id='single-property']/h2/text()").extract_first()
        if "Commercial" not in commercial:
            
            item_loader.add_value("external_link", response.url)

            latitude_longitude = response.xpath("//div[@id='GoogleMap']/following-sibling::script[1]/text()").get()
            if latitude_longitude:
                latitude = latitude_longitude.split('LatLng(')[1].split(',')[0].strip()
                longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip()
        
                item_loader.add_value("latitude", latitude)
                item_loader.add_value("longitude", longitude)


            square_meters = response.xpath("//div[@class='description']/div/ul/li[contains(.,'sqft')]/text()").get()
            if square_meters:
                square_meters = square_meters.split(' ')
                sqm = ''
                for item in square_meters:
                    if item.replace(',', '').strip().isnumeric():
                        sqm = item.replace(',', '').strip()
                        break
                if sqm != '':
                    sqm = str(int(float(sqm) * 0.09290304))
                    item_loader.add_value("square_meters", sqm)

            room_count = response.xpath("//div[@class='description']/div/ul/li[contains(.,'bedro')]/text()").get()
            if room_count:
                try:
                    room_count = w2n.word_to_num(room_count.split('bedroom')[0].strip())
                    room_count = str(int(float(room_count)))
                    item_loader.add_value("room_count", room_count)
                except:
                    pass
            else:
                room_count = response.xpath("//text()[contains(.,'bedroom')]").get()
                if room_count:
                    room_count = room_count.split("bedroom")[0].strip().split(" ")[-1].strip()
                    if room_count and room_count.isdigit():
                        item_loader.add_value("room_count", str(room_count))

            rent = response.xpath("//main[@id='single-property']/h3/strong/text()").get()
            if rent:
                rent = rent.split('£')[1].strip().replace('\xa0', '').replace(',', '').replace('.', '').replace(' ', '')
                item_loader.add_value("rent", rent)
                item_loader.add_value("currency", 'GBP')

            external_id = response.url.split('-')[-1].strip().strip('/')
            if external_id:
                item_loader.add_value("external_id", external_id)

            title = response.xpath("//main[@id='single-property']/h1/text()").extract_first()
            if title:
                item_loader.add_value("title", title)
            titlecheck=item_loader.get_output_value("title")
            if not titlecheck:
                item_loader.add_value("title","St Stephens Close, Canterbury")
            city = response.xpath("//main[@id='single-property']/h1/text()").extract_first()
            if city:
                item_loader.add_value("city", city.split(",")[-1].strip())
                item_loader.add_value("address", city.strip())
            zipcode=response.url
            if zipcode:
                zipcode=zipcode.split("canterbury")[-1].split("-")[-1].replace("/","")
                if len(zipcode)>2:
                    item_loader.add_value("zipcode",zipcode)
                else:
                    zipcode=zipcode.split("property/")[-1].split("-")[0]
                    item_loader.add_value("zipcode",zipcode)

            description = response.xpath("//div[@class='description']/h4/following-sibling::text()").getall()
            desc_html = ''      
            if description:
                for d in description:
                    desc_html += d.strip() + ' '
                desc_html = desc_html.replace('\xa0', '')
                filt = HTMLFilter()
                filt.feed(desc_html)
                item_loader.add_value("description", filt.text)
            
            
            available_date = response.xpath("//text()[contains(.,'Move in')]").get()
            if available_date:
                available_date = available_date.split("Move in")[1].strip()
                date_parsed = dateparser.parse(available_date)
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

            images = [x for x in response.xpath("//div[@class='photos']//img/@src").getall()]
            if images:
                item_loader.add_value("images", images)
                item_loader.add_value("external_images_count", str(len(images)))

            floor_plan_images = [x for x in response.xpath("//div[@class='floorplan']//img/@src").getall()]
            if floor_plan_images:
                item_loader.add_value("floor_plan_images", floor_plan_images)

            furnished = response.xpath("//div[@class='description']/div/ul/li[contains(.,'Furnished')]/text()").get()
            if furnished:
                furnished = True
                item_loader.add_value("furnished", furnished)

            parking = response.xpath("//div[@class='description']/div/ul/li[contains(.,'parking')]/text()").get()
            if parking:
                parking = True
                item_loader.add_value("parking", parking)

            balcony = response.xpath("//div[@class='description']/div/ul/li[contains(.,'balcony')]/text()").get()
            if balcony:
                balcony = True
                item_loader.add_value("balcony", balcony)

            terrace = response.xpath("//div[@class='description']/div/ul/li[contains(.,'terrace')]/text()").get()
            if terrace:
                terrace = True
                item_loader.add_value("terrace", terrace)

            item_loader.add_value("landlord_phone", "01227 767200")
            item_loader.add_value("landlord_email", "lettings@regal-estates.co.uk")
            item_loader.add_value("landlord_name", "Regal Estates")
            
            yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data