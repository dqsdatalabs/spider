# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from  geopy.geocoders import Nominatim
from html.parser import HTMLParser
import dateparser

class MySpider(Spider):
    name = 'oxford_martinco_com' 
    execution_type='testing'
    country='united_kingdom'
    locale='en' 

    def start_requests(self):
        start_urls = [
            {"url": "https://www.martinco.com/property?intent=rent&location=&radius=&type=flats_apartments&price-per=pcm&bedrooms=&sort-by=price-desc&per-page=96&p=1",
             "property_type": "apartment"},
            {"url": "https://www.martinco.com/property?intent=rent&location=&radius=&type=bungalows&price-per=pcm&bedrooms=&sort-by=price-desc&per-page=96&p=1",
            "property_type": "house"},
            {"url": "https://www.martinco.com/property?intent=rent&location=&radius=&type=house_flat_share&price-per=pcm&bedrooms=&sort-by=price-desc&per-page=96&p=1",
             "property_type": "house"},
            {"url": "https://www.martinco.com/property?intent=rent&location=&radius=&type=houses&price-per=pcm&bedrooms=&sort-by=price-desc&per-page=96&p=1",
            "property_type": "house"},
            {"url": "https://www.martinco.com/property?intent=rent&location=&radius=&type=student&price-per=pcm&bedrooms=&sort-by=price-desc&per-page=96&p=1",
            "property_type": "student_apartment"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//a[@class='property-link']/@href").extract():
            follow_url = response.urljoin(item)
            seen = True
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get("property_type")})
        
        if page == 2 or seen:
            follow_url = ""
            if "&p=" in response.url: follow_url = response.url.replace("&p=" + str(page - 1), "&p=" + str(page))
            else: follow_url = response.url + f"&p={page}"
            yield Request(
                url=follow_url,
                callback=self.parse,
                meta={"property_type": response.meta.get("property_type"), "page": page + 1}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get("property_type"))
        unavailable = response.xpath("//strong[@id='propertyPrice']//text()[normalize-space()][contains(.,'Unavailable')]").get()
        if unavailable:
            return
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Oxfordmartinco_PySpider_" + self.country + "_" + self.locale)
        
        title = response.xpath("//h2[@class='text-tertiary']//text()").get()     
        if title:
            item_loader.add_value("title",title.replace("  "," "))

        latitude_longitude = response.xpath("//script[contains(.,'L.marker')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('L.marker([')[1].split(',')[0].strip()
            longitude = latitude_longitude.split('L.marker([')[1].split(',')[1].split(']')[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)

        item_loader.add_xpath("address", "//h2[@class='text-secondary'][@style='font-weight: 400;']/text()")

        square_meters = response.xpath("//li[contains(.,'Sq. Ft.')]/text()").get()
        if not square_meters:
            square_meters = response.xpath("//li[contains(.,'sq. ft.')]/text()").get()
        if not square_meters:
            square_meters = response.xpath("//li[contains(.,'Sq. ft.')]/text()").get()
        if not square_meters:
            square_meters = response.xpath("//li[contains(.,'sq. Ft.')]/text()").get()
        if not square_meters:
            square_meters = response.xpath("//li[contains(.,'sq ft')]/text()").get()
        if not square_meters:
            square_meters = response.xpath("//li[contains(.,'sq.ft.')]/text()").get()
        if not square_meters:
            square_meters = response.xpath("//li[contains(.,'Sq Ft')]/text()").get()
        if not square_meters:
            square_meters = response.xpath("//li[contains(.,'sq ft.')]/text()").get()
        if not square_meters:
            square_meters = response.xpath("//li[contains(.,'ft')]/text()").get()

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

        room = response.xpath("//h2[@class='text-tertiary']//text()[contains(.,'Bedroom')]").get()     
        if room:
            room = room.split("Bedroom")[0].strip()
            if room.isdigit():
                item_loader.add_value("room_count", room)
        else:
            room = response.xpath("//h2[@class='text-tertiary']//text()[contains(.,'Studio')]").get()     
            if room:
                item_loader.add_value("room_count","1")
            
            
        rent = response.xpath("//strong[@id='propertyPrice']/text()").get()
        if rent:
            rent1 = rent.split('|')[0].split('£')[1].split('p')[0].strip().replace('\xa0', '').replace(',', '').replace('.', '')
            rent2 = rent.split('|')[1].split('£')[1].split('p')[0].strip().replace('\xa0', '').replace(',', '').replace('.', '')
            if int(rent1) > int(rent2):
                rent = rent1
            else:
                rent = rent2
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", 'GBP')

        city = response.xpath("//h1/text()").get()
        if city:
            item_loader.add_value("city", city.strip().split(' ')[0].strip())
            zipcode = city.strip().split(' ')[-1].strip()
            if not zipcode.isalpha():
                item_loader.add_value("zipcode", zipcode)
            else:
                zipcode = city.split(",")[-1].split("-")[0].strip()
                item_loader.add_value("zipcode", zipcode)

        external_id = response.url.split('/')[-1]
        if external_id:
            external_id = external_id.strip()
            item_loader.add_value("external_id", external_id)

        description = response.xpath("//div[contains(@class,'property-description')]/text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d.strip() + ' '
            desc_html = desc_html.replace('\xa0', '')
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)

        available_date = response.xpath("//li[contains(.,'Available From')]/text()").get()
        if available_date:
            if len(available_date.split(':')) > 1:
                available_date = available_date.split(':')[1].strip()
                if len(available_date.split('-')) > 2 or len(available_date.split('.')) > 2 or len(available_date.split('/')) > 2:
                    if available_date.isalpha() != True:
                        date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                        date2 = date_parsed.strftime("%Y-%m-%d")
                        item_loader.add_value("available_date", date2)

        images = [x for x in response.xpath("//div[@id='animated-thumbnails']/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        deposit = response.xpath("//li[contains(.,'Security Deposit £')]/text()").get()
        if deposit:
            if len(deposit.split('£')) > 1:
                deposit = str(int(float(deposit.split('£')[1].strip().replace(' ', '').replace(',', ''))))
                item_loader.add_value("deposit", deposit)

        furnished = response.xpath("//li[contains(.,'Furnishing:')]/text()").get()
        if furnished:
            furnished = furnished.split(':')[1]
            if furnished.strip().lower() == 'furnished':
                furnished = True
            elif furnished.strip().lower() == 'unfurnished':
                furnished = False
            if type(furnished) == bool:
                item_loader.add_value("furnished", furnished)
        bathroom = response.xpath("//li[contains(.,'Bathroom')]/text()").get()
        if bathroom:
            bathroom = bathroom.strip().split(' ')[0]
            if bathroom.isdigit():
                item_loader.add_value("bathroom_count", bathroom)
        floor = response.xpath("//li[contains(.,'Floor')]/text()").get()
        if floor:
            floor = floor.strip().split(' ')[0]
            item_loader.add_value("floor", floor)
        
        parking = response.xpath("//li[contains(.,'Parking') or contains(.,'Garage')]//text()").get()
        if parking:
            parking = True
            item_loader.add_value("parking", parking)

        balcony = response.xpath("//li[contains(.,'Balcon')]").get()
        if balcony:
            balcony = True
            item_loader.add_value("balcony", balcony)

        swimming_pool = response.xpath("//li[contains(.,'Swimming Pool')]").get()
        if swimming_pool:
            swimming_pool = True
            item_loader.add_value("swimming_pool", swimming_pool)
        
        washing_machine = response.xpath("//li[contains(.,'Washing machine')]").get()
        if washing_machine:
            washing_machine = True
            item_loader.add_value("washing_machine", washing_machine)

        dishwasher = response.xpath("//li[contains(.,'Dishwasher')]").get()
        if dishwasher:
            dishwasher = True
            item_loader.add_value("dishwasher", dishwasher)

        item_loader.add_value("landlord_name", "Martin & Co Coalville")

        landlord_phone = response.xpath("//strong[.='Call ']/following-sibling::text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
            item_loader.add_value("landlord_phone", landlord_phone)

        landlord_email = response.xpath("//a[contains(@href,'mailto')]/p/text()").get()
        if landlord_email:
            landlord_email = landlord_email.strip()
            item_loader.add_value("landlord_email", landlord_email)
        
        status = response.xpath("//h2/text()[contains(.,'Garage')]").get()
        if not status:
            yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data