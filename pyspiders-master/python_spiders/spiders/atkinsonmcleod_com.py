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
from word2number import w2n

class MySpider(Spider):
    name = 'atkinsonmcleod_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en' 
    start_urls = ["https://www.atkinsonmcleod.com/search-results-list/?increaseRadius=true&radius=1&bedrooms=0&price_range=0-0&location=1&rent"]

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page",2)

        seen = False
        for item in response.xpath("//h2[@class='psearch--address']/a/@href").extract():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
            )
            seen = True
        

        if page == 2 or seen:
            f_url = f"https://www.atkinsonmcleod.com/search-results-list/?resultpage={page}&increaseRadius=true&radius=1&bedrooms=0&price_range=0-0&location=1&rent"
            yield Request(
                url=f_url,
                callback=self.parse,
                meta={
                    "page" : page+1,
                }
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title", "//h1[@class='pfulldetails--address']/text()")

        desc = "".join(response.xpath("//p[@class='summary-description']/text()").getall())
        if desc and ("apartment" in desc or "flat" in desc):
            item_loader.add_value("property_type", "apartment")
        elif desc and "house" in desc:
            item_loader.add_value("property_type", "house")
        else:
            return

        item_loader.add_value("external_source", "Atkinsonmcleod_PySpider_"+ self.country + "_" + self.locale)

        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split(',')[-1].strip().split(' ')[0].strip())
            item_loader.add_value("zipcode", address.split(',')[-1].strip().split(' ')[-1].strip())

        latitude_longitude = response.xpath("//script[contains(.,'latitude')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split("property.latitude  = '")[1].split("'")[0].strip()
            longitude = latitude_longitude.split("property.longitude  = '")[1].split("'")[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)

        square_meters = response.xpath("//li[contains(.,'sq')]/text()").get()
        if square_meters:
            square_meters = square_meters.split(' ')
            sqm = ''
            for item in square_meters:
                if item.strip().isnumeric():
                    sqm = item.strip()
                    break
            if sqm != '':
                sqm = str(int(float(sqm.replace(',', '.')) * 0.09290304))
                item_loader.add_value("square_meters", sqm)

        description = response.xpath("//p[@class='summary-description']/text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d.strip() + ' '
            desc_html = desc_html.replace('\xa0', '')
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)
        
        if desc_html:
            if 'bathroom' in desc_html.lower():
                try:
                    bathroom_count = w2n.word_to_num(desc_html.lower().split('bathroom')[0].strip().split(' ')[-1].strip())
                    item_loader.add_value("bathroom_count", str(bathroom_count))
                except:
                    pass
            if 'floor' in desc_html.lower():
                floor_number = "".join(filter(str.isnumeric, desc_html.lower().split('floor')[0].strip().split(' ')[-1])).strip()
                if floor_number:
                    item_loader.add_value("floor", floor_number)
            else:
                floor = response.xpath("//li[contains(.,'floor')]/text()").get()
                if floor:
                    floor = "".join(filter(str.isnumeric, floor.lower().split('floor')[0])).strip()
                    if floor:
                        item_loader.add_value("floor", floor)
            if 'pet friendly' in desc_html.lower():
                item_loader.add_value("pets_allowed", True)
        
        furnished = response.xpath("//li[.='• Unfurnished']").get()
        if furnished:
            item_loader.add_value("furnished", False)

        furnished = response.xpath("//li[.='• Furnished']").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        terrace = response.xpath("//li[contains(.,'Terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
            
        room_count = response.xpath("//li[contains(.,'Bedroom') or contains(.,'bedroom')]/text()").get()
        if room_count:
            room_count = room_count.split(' ')
            rc = ''
            for item in room_count:
                if item.strip().isnumeric():
                    rc = item.strip()
                    break
            if rc != '':
                item_loader.add_value("room_count", rc)
        elif "bedroom" in desc_html.lower():
            room=desc_html.lower().split("bedroom")[0]
            try:
                room = str(int(float(w2n.word_to_num(room))))
                if room!="0":
                    item_loader.add_value("room_count", room)
            except:
                pass

        rent = response.xpath("//h2[@class='pfulldetails--price']/text()").get()
        if rent:
            rent = rent.strip().split(' ')[0].split('£')[1].strip().replace('\xa0', '')
            rent = rent.replace(',', '').replace('.', '')
            rent = rent.split(' ')
            reg_rent = []
            for i in rent:
                if i.isnumeric():
                    reg_rent.append(i)
            r = "".join(reg_rent)
            r = str(int(r) * 4)
            item_loader.add_value("rent", r)
            item_loader.add_value("currency", 'GBP')

        external_id = response.url.split('/')[-2].strip()
        if external_id:
            external_id = external_id.strip()
            item_loader.add_value("external_id", external_id)

       

        images = [x for x in response.xpath("//nav[@class='pfulldetails--carousel-nav']/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))

        parking = response.xpath("//li[contains(.,'parking')]/text()").get()
        if parking:
            parking = True
            item_loader.add_value("parking", parking)

        balcony = response.xpath("//li[contains(.,'balcon')]/text()").get()
        if balcony:
            balcony = True
            item_loader.add_value("balcony", balcony)

        if "swimming pool" in desc_html.lower():
            item_loader.add_value("swimming_pool",True)
        if "washing machine" in desc_html.lower():
            item_loader.add_value("washing_machine",True)
        if "dishwasher" in desc_html.lower():
            item_loader.add_value("dishwasher",True)
        if "balcony" in desc_html.lower():
            item_loader.add_value("balcony",True)
        if "lift" in desc_html.lower():
            item_loader.add_value("elevator",True)

        item_loader.add_value("landlord_name","ATKINSON MCLEOD")

        landlord_phone = response.xpath("//p[contains(@class,'contact')]/a[contains(@href,'tel')]/text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
            item_loader.add_value("landlord_phone", landlord_phone)
        
        landlord_email = response.xpath("//a[contains(@href,'officeEmail')]/@href").get()
        if landlord_email:
            landlord_email = landlord_email.split('officeEmail=')[1].split('&')[0].strip()
            item_loader.add_value("landlord_email", landlord_email)

        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data

