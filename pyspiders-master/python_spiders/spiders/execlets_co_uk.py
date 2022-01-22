# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'execlets_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.' 
    external_source='Execlets_Co_PySpider_united_kingdom'
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.execlets.co.uk/?id=5344&do=search&for=2&type%5B%5D=8&kwa%5B%5D=&minbeds=0&minprice=0&maxprice=99999999999&id=5344&order=2&page=0&do=search",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.execlets.co.uk/?id=5344&do=search&for=2&type%5B%5D=6&kwa%5B%5D=&minbeds=0&minprice=0&maxprice=99999999999&id=5344&order=2&page=0&do=search",
                    "https://www.execlets.co.uk/?id=5344&do=search&for=2&type%5B%5D=7&kwa%5B%5D=&minbeds=0&minprice=0&maxprice=99999999999&id=5344&order=2&page=0&do=search", 
                    ],
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
                
        for item in response.xpath("//div[@class='property-thumb-info-image']//a//@href").extract():
            follow_url = response.urljoin(item)

            yield Request(
                follow_url,
                callback=self.populate_item,
                meta={"property_type": response.meta.get('property_type')}
            )
        
        next_page = response.xpath("//a[.='Next']/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type": response.meta.get('property_type')}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("pid=")[-1])
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        rented = response.xpath("//span[@class='status_details']/text()[.='Let']").get()
        if rented:
            return
        title = " ".join(response.xpath("//header[@class='property-title']//text()[.!=' Add to favorites'][normalize-space()]").getall())
        if title:
            item_loader.add_value("title", title)
        address = ",".join(response.xpath("//header[@class='property-title']//text()[.!=' Add to favorites'][normalize-space()]").getall())
        if address:
            item_loader.add_value("address", address.replace(" |",","))
        city = response.xpath("//header[@class='property-title']/figure/text()").get()
        if city:
            item_loader.add_value("city", city.split("|")[0].strip())
            item_loader.add_value("zipcode", city.split("|")[1].strip())

        desc = "".join(response.xpath("//div[@id='details']/p//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())

        rent = response.xpath("//dl/dt[.='Price:']/following-sibling::dd[1]//span/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent)

        furnished = response.xpath("//div[@id='details']/text()[contains(.,'Furnished') or contains(.,'furnished')]").get()
        if furnished:
            if "unfurnished" in furnished.lower() or "un-furnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)
        square_meters = response.xpath("//dl/dt[.='Area:']/following-sibling::dd[1]/text()[.!='0.00 m']").get()
        if square_meters:
            square_meters = square_meters.split("m")[0].strip()
            item_loader.add_value("square_meters", int(float(square_meters)))   
        item_loader.add_xpath("room_count", "//dl/dt[.='Bedroom:']/following-sibling::dd[1]//text()")
        item_loader.add_xpath("bathroom_count", "//dl/dt[.='Bathrrom:']/following-sibling::dd[1]//text()")
    
        parking = response.xpath("//dl/dt[.='Parkin:']/following-sibling::dd[1]//text()").get()
        if parking:
            if "yes" in parking.lower():
                item_loader.add_value("parking", True)
            elif "no" in parking.lower():
                item_loader.add_value("parking", False)

        balcony = response.xpath("//div[@id='details']/text()[contains(.,'Balcony')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        deposit = response.xpath("//div[@id='details']//text()[contains(.,'Deposit') or contains(.,'Bond')]").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split(".")[0])
        try:
            energy_label = response.xpath("//div[@id='details']/p//text()[contains(.,'EPC')]").get()
            if energy_label:
                energy_label = energy_label.split("EPC")[-1].split("=")[-1].strip()
                if energy_label[0] in ["A","B","C","D","E","F","G"]:
                    item_loader.add_value("energy_label", energy_label[0])
        except:
            pass

        images = [x for x in response.xpath("//div[@id='galleria']/a/@href").getall()]
        if images:
            item_loader.add_value("images", images) 
        item_loader.add_value("landlord_name", "EXECUTIVE LETS")
        item_loader.add_value("landlord_phone", "0113 267 6966")
        item_loader.add_value("landlord_email", "info@execlets.co.uk")
        yield item_loader.load_item()