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
import re

class MySpider(Spider):
    name = 'danbolig_dk'
    external_source = "Danbolig_PySpider_denmark"
    execution_type='testing'
    country = 'denmark'
    locale='da'
    custom_settings = {
    "HTTPCACHE_ENABLED": False
}
    def start_requests(self):

        headers = {
            "accept": "*/*",
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "en-US,en;q=0.9,tr;q=0.8",
            "content-type": "application/json"
        }
        payload = {"offset":0,"reloadResults":True,"moreResults":False,"nextTileIndex":0,"sortParameter":"Weight","sortDirection":"Desc","isCommercial":False,"propertyAttributes":{"Value":[{"Value":"HasRent","FriendlyValue":"Leje"}]}}
        yield Request(
            "https://danbolig.dk/search/performsearch",
            callback=self.parse,
            body=json.dumps(payload),
            method="POST",
            headers=headers,
            dont_filter=True
        )
    # 1. FOLLOWING
    def parse(self, response):
       

        for item in response.xpath("//li[@class='bolig show action']/div/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
        

    # 2. SCRAPING level 2
    def populate_item(self, response):

        item_loader = ListingLoader(response=response)

        rented = response.xpath("//div[@class='property-label']/text()[.='Let Agreed']").extract_first()
        if rented:return

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", "house")
        item_loader.add_value("external_source", self.external_source)
        title = "".join(response.xpath("//section[@class='description']/h1/text()").extract())
        item_loader.add_value("title", title.strip())

        room = response.xpath("//div[@class='facts']//tr[td[.='Rum']]/td[2]/text()").extract_first()
        if room:
            item_loader.add_value("room_count", room.strip())

        meters = response.xpath("//div[@class='facts']//tr[td[.='Boligareal']]/td[2]/text()").extract_first()
        if meters:
            item_loader.add_value("square_meters", meters.replace("m²","").strip())
        item_loader.add_xpath("external_id", "//div[@class='facts']//tr[td[.='Sagsnummer']]/td[2]/text()")

        available_date = response.xpath(
            "//div[@class='articleBody']/div[@class='detailName' and contains(.,'Aanvaarding')]/following-sibling::div[1]/text()[.!='direct' and .!='in overleg' and .!='per datum']").get()
        if available_date:
            date_parsed = dateparser.parse(
                available_date, date_formats=["%m-%d-%Y"]
            )
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)

        address = " ".join(response.xpath("//div[@class='header']//h4/text()").extract())
        if address:
            item_loader.add_value("address", re.sub("\s{2,}", " ", address))
        
        city_zip = " ".join(response.xpath("//div[@class='header']//h4/text()[2]").extract())
        if city_zip:
            city = city_zip.strip().split(" ")[1].strip()
            zipcode = city_zip.strip().split(" ")[0].strip()
            item_loader.add_value("city", city.strip())
            item_loader.add_value("zipcode", zipcode.strip())
            
        rent = response.xpath("//tr[td[contains(.,'forbrug')]]/td[2]//text()").extract_first()
        if rent:
            price = rent.replace(".","")
            item_loader.add_value("rent", price.strip())
        item_loader.add_value("currency", "DKK")
        images = [ x for x in response.xpath("//li[contains(@class,'image')]/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        description = " ".join(response.xpath("//div[@class='db-description-block']/div/text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.strip())


        longitude = " ".join(response.xpath("//script[@type='application/ld+json']//text()").getall()).strip()   
        if longitude:
            item_loader.add_value("latitude", longitude.split("geo")[1].split("latitude")[1].split(",")[0].replace('"','').replace(":","").strip())
            item_loader.add_value("longitude",longitude.split("geo")[1].split("longitude")[1].split(",")[0].replace('"','').replace(":","").replace("}","").strip())

        deposit = response.xpath("//div[@class='facts show']//tr[td[.='Depositum']]/td[2]/text()").get() 
        if deposit:
            item_loader.add_value("deposit", deposit.replace(".",""))
     
        item_loader.add_xpath("landlord_phone", "//div[@class='description']/p/a/text()")
        item_loader.add_xpath("landlord_name", "//div[@class='description']/p/strong/text()")


        yield item_loader.load_item()