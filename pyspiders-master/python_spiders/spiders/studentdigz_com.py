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
import dateparser
class MySpider(Spider):

    name = 'studentdigz_com'
    start_urls = ["https://www.studentdigz.com/property-search?page=1"]
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    custom_settings = {
        "PROXY_ON": "True"
    }
    
    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)

        seen = False
        for item in response.xpath("//div[@class='property-item-details']/a/@href").extract():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
            )
            seen = True
        
        if page == 2 or seen:
            p_url = f"https://www.studentdigz.com/property-search?page={page}"
            yield Request(
                p_url, 
                callback=self.parse,
                meta={"page":page+1} 
            )
        
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        property_type = response.xpath("//dt[contains(.,'Property type')]/following-sibling::dd[1]/text()").get()
        if property_type:
            if property_type.lower().strip() == 'house':
                item_loader.add_value("property_type", "house")
            elif property_type.lower().strip() == 'flat/apartment':
                item_loader.add_value("property_type", "apartment")
            else:
                item_loader.add_value("property_type", "student_apartment")
        else:
            item_loader.add_value("property_type", "student_apartment")
        item_loader.add_value("external_source", "Studentdigz_PySpider_"+ self.country + "_" + self.locale)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("property/")[1].split("/")[0])

        title = response.xpath("//h1//text()").get()
        if title:
            item_loader.add_value("title", title)

        address = response.xpath("//h1//text()").get()
        if address:
            city_zipcode = address.split(",")[-1].strip()
            city = city_zipcode.split(" ")[0]
            zipcode = city_zipcode.split(city)[1]
            if not zipcode.replace(" ","").isalpha():
                item_loader.add_value("zipcode", zipcode.strip())
            item_loader.add_value("city", city)
            item_loader.add_value("address", address)

        description = response.xpath("//h2[.='Description']/following-sibling::p/text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d.strip() + ' '
            desc_html = desc_html.replace('\xa0', '')
            item_loader.add_value("description", desc_html)

        if 'sq ft' in desc_html.lower() or 'sq. ft.' in desc_html.lower() or 'sqft' in desc_html.lower():
            square_meters = desc_html.lower().split('sq ft')[0].split('sq. ft.')[0].split('sqft')[0].strip().replace('\xa0', '').split(' ')[-1]
            square_meters = str(int(float(square_meters.replace(',', '.').strip('+').strip('(')) * 0.09290304))
            item_loader.add_value("square_meters", square_meters)

        room_count = response.xpath("//div[@id='property-main-details']//span[@title='Maximum tenants']/../../dd[1]/text()").get()
        if room_count:
            room_count = room_count.strip().replace('\xa0', '')
            room_count = str(int(float(room_count)))
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//div[@id='property-main-details']//dt[span[@title='Bathrooms']]/following-sibling::dd[1]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        rent = response.xpath("//div[@id='property-main-price']/strong/text()").get()
        if rent:
            rent = rent.split('£')[1].strip().replace('\xa0', '')
            rent = str(int(float(rent.replace(',', ''))))
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", 'GBP')

        available_date = response.xpath("//div[@id='property-main-details']//dt[.='Available from:']/following-sibling::dd/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"], languages=['en'])
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)

        images = [urljoin('https://www.studentdigz.com', x) for x in response.xpath("//ul[@id='image-gallery']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))

        furnished = response.xpath("//section[@id='property-images']/div/text()").get()
        if furnished:
            if furnished.strip().lower() == 'furnished':
                furnished = True
            elif furnished.strip().lower() == 'unfurnished':
                furnished = False
            if type(furnished) == bool:
                item_loader.add_value("furnished", furnished)

        item_loader.add_value("landlord_name", "Student Digz")

        landlord_phone = response.xpath("//span[@class='phone']/text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
            item_loader.add_value("landlord_phone", landlord_phone)

        landlord_email = response.xpath("//a[contains(@href,'mailto')]/span[2]/text()").get()
        if landlord_email:
            landlord_email = landlord_email.strip()
            item_loader.add_value("landlord_email", landlord_email)
        
      
        yield item_loader.load_item()

