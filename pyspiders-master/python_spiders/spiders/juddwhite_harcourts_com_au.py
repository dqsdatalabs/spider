# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser

class MySpider(Spider):
    name = 'juddwhite_harcourts_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
 
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://juddwhite.harcourts.com.au/Property/rentals?proptype=3&proptype=13&proptype=6&page=1",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://juddwhite.harcourts.com.au/Property/rentals?proptype=5&proptype=7&proptype=11&proptype=12&proptype=14&page=1",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://juddwhite.harcourts.com.au/Property/rentals?proptype=10&page=1",
                ],
                "property_type" : "studio"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//article[contains(@class,'search-item')]//a/@href").getall():
            seen = True
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        if page == 2 or seen: 
            yield Request(response.url.split('&page=')[0] + f"&page={page}", callback=self.parse, meta={"property_type":response.meta["property_type"], "page":page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Juddwhite_Harcourts_Com_PySpider_australia")
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.split("|")[0].strip())
        external_id = response.xpath("//div[@class='listing-features']/p[contains(.,'Listing Number:')]/text()").get()
        if external_id:
            external_id = external_id.split("Listing Number:")[1].strip()
            item_loader.add_value("external_id", external_id)
       
        address = response.xpath("//h1/a[@id='display-location']//text()").get()
        if address:
            address = address.replace(external_id,"")
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split(",")[0].strip())   
 
        item_loader.add_xpath("room_count", "//div[@class='listing-feature-icons']//div[contains(@data-tooltip,'Bedroom')]/span/text()")
        item_loader.add_xpath("bathroom_count", "//div[@class='listing-feature-icons']//div[contains(@data-tooltip,'Bathroom')]/span/text()")

        deposit = response.xpath("//div[@id='property-feature-list']/div/div[span[.='Bond $: ']]/following-sibling::div[1]/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.replace(",",""))
        rent = response.xpath("//h3/span[contains(.,'$')]/text()").get()
        if rent:
            if " p.w." in rent:
                rent = rent.split('$')[-1].split('p')[0].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent)) * 4))
            else:       
                rent = rent.split('$')[-1].split('p')[0].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent))))
        item_loader.add_value("currency", 'USD')

        available_date = response.xpath("//div[@id='property-feature-list']/div/div[span[.='Available Date: ']]/following-sibling::div[1]/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d %m %Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        description = " ".join(response.xpath("//div[@id='listing-description-content']/p//text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())

        furnished = response.xpath("//div[@id='property-feature-list']/div/div[span[.='Property Features: ']]/following-sibling::div[1]/text()").get()
        if furnished:
            if furnished.lower().strip() == "unfurnished":
                item_loader.add_value("furnished",False)
            elif furnished.lower().strip() == "furnished":
                item_loader.add_value("furnished",True)
 
        images = [x for x in response.xpath("//div[@class='swiper-wrapper']/div[@class='swiper-slide']/div/@data-background").getall()]
        if images:
            item_loader.add_value("images", images)
            
        parking = response.xpath("//div[@class='listing-feature-icons']//div[contains(@data-tooltip,'Off street') or contains(@data-tooltip,'Garage') or contains(@data-tooltip,'car spaces')]/span/text()").get()
        parking2 = response.xpath("//div/@data-tooltip[contains(.,'Car')]").get()
        if parking or parking2:
            item_loader.add_value("parking", True)
            
        latlng = response.xpath("//script[@type='text/javascript'][contains(.,'latitude')]/text()").get()
        if latlng:
            item_loader.add_value("latitude", latlng.split("'latitude',")[1].split(");")[0].strip())
            item_loader.add_value("longitude", latlng.split("'longitude',")[1].split(");")[0].strip())
        landlord_phone = response.xpath("//div[@id='listing-enquiry-form']//div[@class='phone hc-text']//a/text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.strip())
        item_loader.add_xpath("landlord_name", "//div[@id='listing-enquiry-form']//div[@class='staff-name']/p/text()")
        item_loader.add_value("landlord_email", "info@juddwhite.com.au")     
 
        yield item_loader.load_item()