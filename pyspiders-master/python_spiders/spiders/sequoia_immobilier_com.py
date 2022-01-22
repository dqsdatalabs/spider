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
import dateparser

class MySpider(Spider):
    name = 'sequoia_immobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    post_url = "http://www.sequoia-immobilier.com/en/search/"
    current_index = 0
    other_prop = ["2"]
    other_prop_type = ["house"]
    external_source="Sequoia_Immobilier_Com_PySpider_france"
    def start_requests(self):
        formdata = {
            "nature": "2",
            "type[]": "1",
            "price": "",
            "currency": "EUR",
            "customroute": "",
            "homepage": "1",
        }
        yield FormRequest(
            url=self.post_url,
            callback=self.parse,
            dont_filter=True,
            formdata=formdata,
            meta={
                "property_type":"apartment",
            }
        )

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        for item in response.xpath("//li//a[@class='button']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        if response.xpath("//li[@class='nextpage']/a/@href").get():
            p_url = f"http://www.sequoia-immobilier.com/en/search/{page}"
            yield Request(p_url, dont_filter=True, callback=self.parse, meta={"property_type":response.meta["property_type"], "page":page+1})
        elif self.current_index < len(self.other_prop):
            formdata = {
                "nature": "2",
                "type[]": self.other_prop[self.current_index],
                "price": "",
                "currency": "EUR",
                "customroute": "",
                "homepage": "1",
            }
            yield FormRequest(
                url=self.post_url,
                callback=self.parse,
                dont_filter=True,
                formdata=formdata,
                meta={
                    "property_type":self.other_prop_type[self.current_index],
                }
            )
            self.current_index += 1

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title", "//title/text()")
        item_loader.add_xpath("external_id", "substring-after(//ul/li[contains(.,'Ref')]/text(),'. ')")
        item_loader.add_xpath("latitude", "substring-before(substring-after(//script[contains(.,'marker_map_2')]/text(),'marker_map_2 = L.marker(['),',')")
        item_loader.add_xpath("longitude", "substring-before(substring-after(substring-after(//script[contains(.,'marker_map_2')]/text(),'marker_map_2 = L.marker(['),', '),']')")

        rent = response.xpath("//section[@class='showPictures']/article//ul/li[contains(.,'€')]/text()").extract_first()
        if rent:
            rent=rent.replace(",","")
            item_loader.add_value("rent_string", rent.split("/")[0].strip())

        utilities = response.xpath("//div[@class='legal details']/ul/li[contains(.,'Fees')]/span/text()").extract_first()
        if utilities:
            item_loader.add_value("utilities", utilities.split("/")[0].strip())

        deposit = response.xpath("//div[@class='legal details']/ul/li[contains(.,'Guarantee')]/span/text()").extract_first()
        if deposit:
            item_loader.add_value("deposit", deposit.replace(",","").strip())

        room_count = "".join(response.xpath("//div[@class='summary details']/ul/li[contains(.,'Room')]/span/text()").extract())
        if room_count:
            item_loader.add_value("room_count",room_count.split(" ")[0].strip())

        bathroom_count = "".join(response.xpath("//div[@class='areas details']/ul/li[contains(.,'Bathroom')]/text()").extract())
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.split(" ")[0].strip())

        energy_label = "".join(response.xpath("//div[@class='diagnostics details']/img[@alt='Energy - Conventional consumption']/@src").extract())
        if energy_label:
            item_loader.add_value("energy_label",energy_label.split("/")[-1].split("%")[0].strip())

        square_meters = response.xpath("//div[@class='summary details']/ul/li[contains(.,'Surface')]/span/text()").extract_first()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split(" ")[0].strip())

        available_date=response.xpath("//div[@class='summary details']/ul/li[contains(.,'Available') or contains(.,'Availability ')]/span/text()[.!='Free' and .!='Occupied']").get()

        if available_date:
            date2 =  available_date.strip()
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)

        address = response.xpath("substring-after(//div[@class='titles']/h1/text(),'- ')").extract_first()
        if address:
            item_loader.add_value("address",address.strip())

        images = [x for x in response.xpath("//section[@class='showPictures']/div//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)

        floor = response.xpath("//div[@class='summary details']/ul/li[contains(.,'Floor')]/span/text()").extract_first()
        if floor:
            item_loader.add_value("floor",floor.split(" ")[0].strip())

        swimming_pool = "".join(response.xpath("//div[@class='services details']/ul/li[contains(.,'Swimming pool')]/text()").extract())
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)

        elevator = "".join(response.xpath("//div[@class='services details']/ul/li[contains(.,'Lift')]/text()").extract())
        if elevator:
            item_loader.add_value("elevator", True)
      
        terrace = "".join(response.xpath("//div[@class='services details']/ul/li[contains(.,'Lift')]/text()").extract())
        if terrace:
            item_loader.add_value("terrace", True)

        parking = "".join(response.xpath("//div[@class='areas details']/ul/li[contains(.,'Parking')]/text() | //div[@class='proximities details']/ul/li[contains(.,'parking')]/text()").extract())
        if parking:
            item_loader.add_value("parking", True)

        city = response.xpath("//title/text()").get()
        if city:
            item_loader.add_value("city", city.split("-")[1].strip())

        item_loader.add_value("landlord_name", "Sequoia Immobilier")
        item_loader.add_value("landlord_phone", "+33 4 93 74 63 03")
        item_loader.add_value("landlord_email", "contact@sequoia-immobilier.com")
        yield item_loader.load_item()