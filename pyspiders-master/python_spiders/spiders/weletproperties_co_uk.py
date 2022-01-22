# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re

class MySpider(Spider):
    name = 'weletproperties_co_uk'    
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.weletproperties.co.uk/properties.aspx?Mode=1&PropertyTypeGroup=2&PriceMin=0&PriceMax=0&Bedrooms=0&ShowSearch=1",
                ],
                "property_type" : "apartment",
                "type_group" : "Flats/Apartments"
            },
            {
                "url" : [
                    "https://www.weletproperties.co.uk/properties.aspx?Mode=1&PropertyTypeGroup=1&PriceMin=0&PriceMax=0&Bedrooms=0&ShowSearch=1",
                ],
                "property_type" : "house",
                "type_group" : "Houses"

            },   
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "type_group":url.get("type_group")})


    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)

        seen = False
        for item in response.xpath("//div/a[contains(@id,'PropertyURL')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            view_state = response.xpath("//input[@name='__VIEWSTATE']/@value").get()
            view_state_gen = response.xpath("//input[@name='__VIEWSTATEGENERATOR']/@value").get()
            event_val = response.xpath("//input[@name='__EVENTVALIDATION']/@value").get()
            type_group = response.meta.get("type_group")
            total_page = response.xpath("//div[@class='pagination']/ul/li[@class][last()]/a/text()").get().strip()

            formdata = {
                "__EVENTTARGET": "ctl00$ContentPlaceHolderMain$lnkPageNext",
                "__EVENTARGUMENT": "",
                "__LASTFOCUS": "",
                "__VIEWSTATE": view_state,
                "__VIEWSTATEGENERATOR": view_state_gen,
                "__EVENTVALIDATION": event_val,
                "ctl00$ContentPlaceHolderMain$uctPropertySearch$txtSearch": "",
                "ctl00$ContentPlaceHolderMain$uctPropertySearch$cboPropertyTypeGroup": type_group,
                "ctl00$ContentPlaceHolderMain$uctPropertySearch$cboBedrooms": "0",
                "ctl00$ContentPlaceHolderMain$uctPropertySearch$cboMinPrice": "0",
                "ctl00$ContentPlaceHolderMain$uctPropertySearch$cboMaxPrice": "0",
                "ctl00$ContentPlaceHolderMain$uctPropertySearch$cboStatus": "Show All",
                "ctl00$ContentPlaceHolderMain$lstSort": "Sort Highest Price",
                "ctl00$ContentPlaceHolderMain$cboPageNos": f"Page {page} of {total_page}",
            }

            yield FormRequest(
                url="https://www.weletproperties.co.uk/properties.aspx?Mode=1&PropertyTypeGroup=2&PriceMin=0&PriceMax=0&Bedrooms=0&ShowSearch=1",
                callback=self.parse,
                formdata=formdata,
                meta={"page":page+1, "type_group":type_group, 'property_type': response.meta.get('property_type')}
            )
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Weletproperties_PySpider_" + self.country + "_" + self.locale)

        status = response.xpath("//span[contains(@id,'Status')]/text()").get()
        if status and ("under offer" in status.lower() or "let" in status.lower()):
            return

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("city", response.url.split("-pi")[0].split("-")[-1].capitalize())

        title = "".join(response.xpath("//div[@class='col-sm-12']/h1/text()").extract())
        if title:
            item_loader.add_value("title",title )
            item_loader.add_value("address",title )

        room = "".join(response.xpath("//ul[@class='amenities']/li/i[@class='icon-bedrooms']/following-sibling::text()").extract())
        if room:
            item_loader.add_value("room_count", room.strip())

        bathroom = "".join(response.xpath("//ul[@class='amenities']/li/i[@class='icon-bathrooms']/following-sibling::text()").extract())
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom.strip())

        meters = "".join(response.xpath("substring-before(//ul[@class='amenities']/li/i[@class='icon-area']/following-sibling::text(),'/')").extract())
        if meters:
            item_loader.add_value("square_meters", meters.strip().split("m²")[0])

        price = "".join(response.xpath("substring-after(//div[contains(@class,'price')]/span[1]/text(),'Price ')").extract())
        if price:
            rent = price.replace(",",".")
            item_loader.add_value("rent_string", rent.split("pcm")[0].strip().replace(" ",""))

        floor = "".join(response.xpath("//ul[contains(@class,'property-features')]/li[contains(.,'Floor')]/text()[not(contains(.,'Swimming Pool'))]").extract())
        if floor:
            floor = floor.split("Floor")[0].replace("One Bedroom","")
            item_loader.add_value("floor",floor)
        
        item_loader.add_xpath("external_id","//div[contains(@class,'share-wraper')]/div/p/span/text()")

        import dateparser
        available_date = response.xpath("//div[contains(@class,'main')]/h2/text()[contains(.,'Available') or contains(.,'AVAILABLE')]").get()
        if available_date and "now" not in available_date.lower():
            available_date = available_date.split("Available")[1].split("-")[0].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

        if " furnished" in available_date.lower():
            item_loader.add_value("furnished", True)
        
        desc = " ".join(response.xpath("//div[@id='tabDescription']/p/span/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        images=[x for x in response.xpath("//div[@class='owl-carousel']/div/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))

        latlong = response.xpath("normalize-space(//iframe/@src)").extract_first()
        if latlong:
            item_loader.add_xpath("latitude", "substring-before(substring-after(normalize-space(//iframe/@src),'cbll='),',')")
            item_loader.add_xpath("longitude", "substring-before(substring-after(normalize-space(//iframe/@src),','),'&')")

        swimming = "".join(response.xpath("//ul[contains(@class,'property-features')]/li/text()[contains(.,'Swimming Pool')]").extract())
        if swimming:
            item_loader.add_value("swimming_pool", True)


        balcony = "".join(response.xpath("//ul[contains(@class,'property-features')]/li/text()[contains(.,'Balcony')]").extract())
        if balcony:
            item_loader.add_value("balcony", True)

        parking = "".join(response.xpath("//ul[contains(@class,'property-features')]/li/text()[contains(.,'Parking')]").extract())
        if parking:
            item_loader.add_value("parking", True)

        terrace = "".join(response.xpath("//ul[contains(@class,'property-features')]/li/text()[contains(.,'Terrace')]").extract())
        if terrace:
            item_loader.add_value("terrace", True)

        furnished = "".join(response.xpath("//ul[contains(@class,'property-features')]/li/text()[contains(.,'Furnished')]").extract())
        if furnished:
            item_loader.add_value("furnished", True)
        else:
            unfurnished = "".join(response.xpath("//ul[contains(@class,'property-features')]/li/text()[contains(.,'Unfurnished')]").extract())
            if unfurnished:
                item_loader.add_value("furnished", False)

        item_loader.add_value("landlord_email", "info@weletproperties.co.uk")
        item_loader.add_value("landlord_phone", "0161 839 2372")
        item_loader.add_value("landlord_name", "Weletpropertie")

        yield item_loader.load_item()
