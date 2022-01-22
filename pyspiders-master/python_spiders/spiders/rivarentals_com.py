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
import re


class MySpider(Spider):
    name = 'rivarentals_com'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'en'

    def start_requests(self):
        start_urls = [
            {"url": "https://www.rivarentals.com/rotterdam-apartments-and-houses/?action=search&house_type=Apartment&min-price=500", "property_type": "apartment"},
            {"url": "https://www.rivarentals.com/rotterdam-apartments-and-houses/?action=search&house_type=Groundfloor+apartment&min-price=500", "property_type": "apartment"},
            {"url": "https://www.rivarentals.com/rotterdam-apartments-and-houses/?action=search&house_type=Upstairs+apartment&min-price=500", "property_type": "apartment"},
            {"url": "https://www.rivarentals.com/rotterdam-apartments-and-houses/?action=search&house_type=Family+house&min-price=500", "property_type": "house"},
            {"url": "https://www.rivarentals.com/rotterdam-apartments-and-houses/?action=search&house_type=Terraced+house&min-price=500", "property_type": "house"},
            {"url": "https://www.rivarentals.com/rotterdam-apartments-and-houses/?action=search&house_type=Canal+house&min-price=500&rented=0", "property_type": "house"},
            {"url": "https://www.rivarentals.com/rotterdam-apartments-and-houses/?action=search&house_type=Corner+house&min-price=500", "property_type": "house"},
            {"url": "https://www.rivarentals.com/rotterdam-apartments-and-houses/?action=search&house_type=Villa&min-price=500", "property_type": "house"},
            {"url": "https://www.rivarentals.com/rotterdam-apartments-and-houses/?action=search&house_type=Semi-detached+house&min-price=500", "property_type": "house"},
            {"url": "https://www.rivarentals.com/rotterdam-apartments-and-houses/?action=search&house_type=Manor+house&min-price=500", "property_type": "house"},
            {"url": "https://www.rivarentals.com/rotterdam-apartments-and-houses/?action=search&house_type=Penthouse&min-price=500", "property_type": "house"},
            {"url": "https://www.rivarentals.com/rotterdam-apartments-and-houses/?action=search&house_type=Maisonnette&min-price=500", "property_type": "house"},
            {"url": "https://www.rivarentals.com/rotterdam-apartments-and-houses/?action=search&house_type=Studio&min-price=500", "property_type": "studio"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            "base_url":url.get('url')})

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//ul[@class='house-overview-filtered-houses']/li/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
        pagination = response.xpath("//div[@class='overview-pagination']/a[last()]/@href").get()
        if pagination:
            url = response.urljoin(pagination)
            yield Request(url, callback=self.parse, meta={'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Rivarentals_PySpider_" + self.country + "_" + self.locale)
        
        title = " ".join(response.xpath("//div[@class='tab-title']/h2/text()").extract())
        item_loader.add_value("title", title)
        item_loader.add_value("external_link", response.url)

        price = response.xpath("//table//tr/td[@class='price']/text()[contains(., '€')]").extract_first()
        if price:
            item_loader.add_value("rent", price.split("€")[1].split(",")[0])
            item_loader.add_value("currency", "EUR")

        item_loader.add_value("property_type", response.meta.get("property_type"))

        square = response.xpath("//tr[td[.='Floor space']]/td[2]/text()").get()
        if square:
            item_loader.add_value("square_meters", square.split("m")[0])

        images = [response.urljoin(x)for x in response.xpath("//div[@class='swiper-wrapper']/div[contains(@class,'swiper-slide') and not(contains(@class,'swiper-slide-duplicate'))]/a/@href[.!='']").extract()]
        if images:
                item_loader.add_value("images", list(set(images)))
                
        
        item_loader.add_xpath("bathroom_count","//tr[td[.='Bathrooms']]/td[2]/text()[.!='0']")

        available_date = response.xpath("normalize-space(//tr[td[.='Available from']]/td[2]/text()[. !='Immediately'])").extract_first()
        if available_date:
            if " - " in available_date:
                ava = available_date.split(" - ")[0]
                date_parsed = dateparser.parse(ava, date_formats=["%d %B %Y"])
                date1 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date1)
            else:
                date_parsed = dateparser.parse(available_date, date_formats=["%d %B %Y"])
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

        desc = "".join(response.xpath("//div[@class='tab-info']/p//text()[not(parent::h3)]").extract())
        if desc:
            item_loader.add_value("description", desc)
            if "washing machine" in desc:
                item_loader.add_value("washing_machine", True)
            if "elevator" in desc:
                item_loader.add_value("elevator", True)
            if "parking" in desc:
                item_loader.add_value("parking", True)
        room_count = response.xpath("//tr[td[.='Bedrooms']]/td[2]/text()[.!='0']").get()
        if room_count:
            item_loader.add_value("room_count", room_count)   
        elif not room_count:
            if "studio" in desc:
                item_loader.add_value("room_count", "1")    
        furnished = response.xpath("//tr[td[.='Interior']]/td[2]//text()").get()
        if furnished:
            if "Furnished" in furnished:
                item_loader.add_value("furnished", True)
            if "Decorated" in furnished:
                pass
            else:
                item_loader.add_value("furnished", False)

        balcony = response.xpath("//ul[@class='tab-icons-list']/li/p/text()[contains(.,'Balcony')]").extract_first()
        if balcony:
            if "Balcony" in balcony:
                item_loader.add_value("balcony", True)
            else:
                item_loader.add_value("balcony", False)
        terrace = response.xpath("//ul[@class='tab-icons-list']/li/p/text()[contains(.,'Terrace')]").extract_first()
        if terrace:
            if "Terrace" in terrace:
                item_loader.add_value("terrace", True)
            else:
                item_loader.add_value("terrace", False)

        address = " ".join(response.xpath("//div[@class='tab-title']/h2/text()").extract())
        if address:
            item_loader.add_value("address", address)

        item_loader.add_xpath("city", "//div[@class='tab-title']/h2/text()[2]")
        
        item_loader.add_xpath("latitude", "//section[@id='google-maps']/div[@class='wide-map']/@data-latitude")
        item_loader.add_xpath("longitude", "//section[@id='google-maps']/div[@class='wide-map']/@data-longitude")

        phone = response.xpath('//div[@class="contact-holder"]/a/@href[contains(., "tel:")]').get()
        if phone:
            item_loader.add_value("landlord_phone", phone.replace("tel:", ""))
        email = response.xpath('//div[@class="contact-holder"]/a/@href[contains(., "mailto")]').get()
        if email:
            item_loader.add_value("landlord_email", email.replace("mailto:", ""))
        item_loader.add_value("landlord_name", "Rivarentals")

        yield item_loader.load_item()