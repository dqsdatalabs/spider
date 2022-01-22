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
    name = 'cswonen_nl'
    execution_type='testing'
    country='netherlands'
    locale='nl'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.cswonen.nl/objecten/particulier/?_type=appartement",
                    "https://www.cswonen.nl/objecten/particulier/?_type=benedenwoning",
                    "https://www.cswonen.nl/objecten/particulier/?_type=bovenwoning",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.cswonen.nl/objecten/particulier/?_type=1-gezinswoning",
                    "https://www.cswonen.nl/objecten/particulier/?_type=tussenwoning",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.cswonen.nl/objecten/particulier/?_type=kamer",
                ],
                "property_type" : "room"
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
        for item in response.xpath("//div[@class='housesoverview-blk ']"):
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True

        if page == 2 or seen:
            p_url = response.url.split("&_paged=")[0] + f"&_paged={page}"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1, "property_type":response.meta["property_type"]}
            )     
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title", "//title/text()")
        item_loader.add_value("external_source", "Cswonen_PySpider_netherlands")

        rent = "".join(response.xpath("//div[@class='houseinfo-price']/strong/text()").extract())
        if rent:
            price =  rent.replace(" ","").split("(")[0].strip()
            item_loader.add_value("rent_string", price)
        else:
            item_loader.add_value("currency", "EUR")

        deposit = "".join(response.xpath("//div[@class='houseinfo-price']/strong/text()").extract())
        if deposit:
            dep =  deposit.replace("Borg","").strip()
            item_loader.add_value("deposit", dep)

        meters = "".join(response.xpath("//table//tr[td[.='Oppervlakte']]/td[2]/strong/text()").extract())
        if meters:
            item_loader.add_value("square_meters", meters.split("m")[0].strip())

        room = "".join(response.xpath("//table//tr[td[.='Aantal slaapkamers']]/td[2]/strong/text()").extract())
        if room:
            item_loader.add_value("room_count", room.strip())
        else:
            item_loader.add_value("room_count", "1")
        bathroom_count = "".join(response.xpath("//table//tr[td[.='Aantal badkamers']]/td[2]/strong/text()").extract())
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        address = "".join(response.xpath("//table//tr[td[.='Adres']]/td[2]/strong/text()").extract())
        if address:
            city = address.split(",")[1].strip().split(" ")[1]
            zipcode = address.split(",")[1].strip().split(" ")[0]
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", city.strip())
            item_loader.add_value("zipcode", zipcode.strip())

        latlng = "".join(response.xpath("//li/a/@href[contains(.,'map')]").extract())
        if latlng:
            item_loader.add_xpath("latitude", "substring-before(substring-after(//li/a/@href[contains(.,'map')],'Wonen/@'),',')")
            item_loader.add_xpath("longitude", "substring-before(substring-after(//li/a/@href[contains(.,'map')],','),',')")
            
        desc = "".join(response.xpath("//div[@class='col-md-12 col-lg-7']/p/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        images = [x for x in response.xpath("//div[@class='slick-slide']//a/@href").extract()]
        if images is not None:
            item_loader.add_value("images", images) 

        date2 = ""
        available_date="".join(response.xpath("//div[@class='houseinfo-monts']/p[1]/text()").getall())
        if available_date:
            if "per" in available_date:
                date2 = available_date.split("per")[1].strip()
                date2 =  available_date.split("per")[1]
                date_parsed = dateparser.parse(
                    date2, date_formats=["%m-%d-%Y"]
                )
                date3 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date3)

        elevator = "".join(response.xpath("//table//tr[td[.='Lift']]/td[2]/strong/text()").extract())
        if elevator:
            if "Ja" in elevator:
                item_loader.add_value("elevator", True)
            elif "Nee" in elevator:
                item_loader.add_value("elevator", False)

        balcony = "".join(response.xpath("//table//tr[td[.='Balkon']]/td[2]/strong/text()").extract())
        if balcony:
            item_loader.add_value("elevator", True)
        elif "Geen" in balcony:
            item_loader.add_value("elevator", False)

        furnished = "".join(response.xpath("//table//tr[td[.='Inrichting']]/td[2]/strong/text()").extract())
        if furnished:
            if "Kaal"  not in furnished:
                item_loader.add_value("furnished", True)
            elif "Kaal" in furnished:
                item_loader.add_value("furnished", False)

        dishwasher = "".join(response.xpath("//table//tr[td[.='Afwasmachine']]/td[2]/strong/text()").extract())
        if dishwasher:
            if "Ja" in dishwasher:
                item_loader.add_value("dishwasher", True)
            elif "Nee" in furnished:
                item_loader.add_value("dishwasher", False)


        terrace = "".join(response.xpath("//table//tr[td[.='Dakterras']]/td[2]/strong/text()").extract())
        if terrace:
            if "Ja" in terrace:
                item_loader.add_value("terrace", True)
            elif "Nee" in terrace:
                item_loader.add_value("terrace", False)

        item_loader.add_value("external_id", response.url.split("-")[-1].split("/")[0])
        
        item_loader.add_value("landlord_phone", "0165-870000")
        item_loader.add_value("landlord_name", "CS Wonen")
        item_loader.add_value("landlord_email", "info@cswonen.nl")  


        yield item_loader.load_item()