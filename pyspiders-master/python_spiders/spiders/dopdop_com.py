# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
from word2number import w2n
import re

class MySpider(Spider):
    name = 'dopdop_com'
    start_urls = ['https://www.dop-dop.com/properties?page=1']
    execution_type = 'testing'
    country = 'netherlands'
    external_source='Dopdop_PySpider_netherlands_en'
    locale = 'en'  # LEVEL 1

    def start_requests(self):
        start_urls = [
            {
                "url" : "https://www.dop-dop.com/properties?house_type=Apartment",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.dop-dop.com/properties?house_type=Upstairs+apartment",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.dop-dop.com/properties?house_type=Double+floor+apartment",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.dop-dop.com/properties?house_type=Groundfloor+apartment",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.dop-dop.com/properties?house_type=Penthouse",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.dop-dop.com/properties?house_type=Maisonnette",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.dop-dop.com/properties?house_type=Studio",
                "property_type" : "studio"
            },
            {
                "url" : "https://www.dop-dop.com/properties?house_type=Bungalow",
                "property_type" : "house"
            },
            {
                "url" : "https://www.dop-dop.com/properties?house_type=2-onder-1-kap+woning",
                "property_type" : "house"
            },
            {
                "url" : "https://www.dop-dop.com/properties?house_type=Family+house",
                "property_type" : "house"
            },
            {
                "url" : "https://www.dop-dop.com/properties?house_type=House+boat",
                "property_type" : "house"
            },
            {
                "url" : "https://www.dop-dop.com/properties?house_type=Manor+house",
                "property_type" : "house"
            },
            {
                "url" : "https://www.dop-dop.com/properties?house_type=Tussenwoning",
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//div[contains(@class,'listing-slider')]"):
            status = item.xpath("./p[@class='label']/text()").get()
            if status and ("leased" in status.lower() or "sold" in status.lower()):
                continue
            follow_url = response.urljoin(item.xpath("./@data-url").get())
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            url = response.url.split("&page")[0] + f"&page={page}"
            yield Request(url, callback=self.parse, meta={"page": page+1, 'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Dopdop_PySpider_" + self.country + "_" + self.locale)

        title = response.xpath("//h1//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        item_loader.add_value("external_link", response.url)


        prop =  response.xpath("//div[@class='features']/ul/li[span[.='House type']]/span[@class='feature-desc']/text()[. !='-']").extract_first()
        if prop:
            price = response.xpath("normalize-space(//p[@class='price']/text())").extract_first()
            if price:
                price = price.split("€")[1].split("(")[0].replace('\xa0', '').replace(' ', '').replace(',', '').replace('.', '')
                item_loader.add_value("rent", price)
                item_loader.add_value("currency", "EUR")

            external_id = response.url.split('-')[-1].strip()
            if external_id:
                item_loader.add_value("external_id", external_id)
            

            deposit = response.xpath("//li[span[.='Deposit']][1]/span[@class='feature-desc']/text()").extract_first()
            if deposit:
                item_loader.add_value("deposit", deposit.split("€")[1].replace('\xa0', '').replace(' ', '').replace(',', '').replace('.', ''))
                item_loader.add_value("property_type", response.meta.get('property_type'))
                
            room = response.xpath("//span[@class='bedrooms']/text()").extract_first()
            if room:
                room = room.split(" ")[0].strip()
                if room != "0":
                    item_loader.add_value("room_count", room)

            house_type = response.xpath("normalize-space(//li[span[.='House type']]/span[2]/text())").extract_first()
            if house_type == "Studio":
                item_loader.add_value("room_count", "1")

            bathrooms = response.xpath("normalize-space(//li[span[.='Bathrooms']]/span[2]/text())").extract_first()
            
            if bathrooms:
                item_loader.add_value("bathroom_count", bathrooms)

            square = response.xpath("normalize-space(//div[@class='features']/ul/li[span[.='Surface']]/span[@class='feature-desc']/text())").get()
            if square:
                item_loader.add_value("square_meters", square.split("m²")[0])

            images = [response.urljoin(x)for x in response.xpath("//ul/li[@class='slide image-slide no-crop']/@data-src").extract()]
            if images:
                item_loader.add_value("images", images)
                item_loader.add_value("external_images_count", len(images))

            available_date = response.xpath("//li[span[.='Available']][1]/span[@class='feature-desc']/text()").extract_first()
            if available_date is not None:
                if "Now" not in available_date:
                    date_parsed = dateparser.parse(available_date, date_formats=["%d %B %Y"])
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

            desc = "".join(response.xpath("//p[@class='description-text']/text()").extract())
            item_loader.add_value("description", desc)

            if desc:
                if 'floor' in desc.lower():
                    try:
                        floor = w2n.word_to_num(desc.lower().split('floor')[0].strip().split(' ')[-1].strip().rstrip('st').rstrip('nd').rstrip('rd').rstrip('th'))
                        item_loader.add_value("floor", str(floor))
                    except:
                        pass
                if 'swimming pool' in desc.lower():
                    item_loader.add_value("swimming_pool", True)

            terrace = "".join(response.xpath("//li[span[.='Balcony']]/span[@class='feature-desc']/text()").extract())
            if terrace:
                item_loader.add_value("balcony", True)

            terrace = "".join(response.xpath("//li[span[.='Roof terrace' or .='Terrace' ]]/span[@class='feature-desc']/text()").extract())
            if terrace:
                if "Yes" in terrace:
                    item_loader.add_value("terrace", True)
                else:
                    item_loader.add_value("terrace", False)

            terrace = "".join(response.xpath("//li[span[.='Parking']]/span[@class='feature-desc']/text()").extract())
            if terrace:
                item_loader.add_value("parking", True)

            terrace = "".join(response.xpath("//li[span[.='Elevator']]/span[@class='feature-desc']/text()").extract())
            if terrace:
                item_loader.add_value("elevator", True)

            
            terrace = "".join(response.xpath("//li[span[.='Interior']]/span[@class='feature-desc']/text()[. !='Unfurnished']").extract())
            if terrace:
                item_loader.add_value("furnished", True)


            terrace = "".join(response.xpath("//li[span[.='Dishwasher']]/span[@class='feature-desc']/text()").extract())
            if terrace:
                    item_loader.add_value("dishwasher", True)

            terrace = "".join(response.xpath("//li[span[.='Washing machine']]/span[@class='feature-desc']/text()").extract())
            if terrace:
                item_loader.add_value("washing_machine", True)

            item_loader.add_xpath("address","//section//p[@class='address']/text()")
            item_loader.add_xpath("city", "//section//p[@class='city-zipcode']/text()")

            item_loader.add_xpath("latitude", "//section[@class='map-block']/div[@id='mapview-canvas']/@data-lat")
            item_loader.add_xpath("longitude", "//section[@class='map-block']/div[@id='mapview-canvas']/@data-lng")

            item_loader.add_xpath("landlord_phone", "//p[@class='contact-box-2']/span[1]/a/text()")
            item_loader.add_xpath("landlord_email", "//p[@class='contact-box-2']/span[2]/a/text()")
            item_loader.add_value("landlord_name", "Dop Dop")

            yield item_loader.load_item()