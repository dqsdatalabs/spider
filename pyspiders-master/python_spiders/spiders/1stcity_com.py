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
    name = '1city_com'
    execution_type='testing'
    country='australia'
    locale='en'
    start_urls = ["https://1stcity.com.au/results"]

    def parse(self,response):

        token = response.xpath("//meta[@name='csrf-token']/@content").extract_first()
        prop_type = ["Apartment","House"]

        for p in prop_type:

            formdata = {
                "authenticityToken": f"{token}",
                "_method": "post",
                "office_id": "",
                "listing_suburb_search": "",
                "LISTING_SALE_METHOD": "Lease",
                "surrounding": "true",
                "listing_property_type": f"{p}",
                "LISTING_PRICE_FROM": "",
                "LISTING_PRICE_TO": "",
                "LISTING_BEDROOMS": "",
                "LISTING_BATHROOMS": "",
                "LISTING_CARSTOTAL": "",
            }

            yield FormRequest(
                url="https://1stcity.com.au/results",
                callback=self.parse_list,
                formdata=formdata,
                meta={"property_type":p}
            )
 
    # 1. FOLLOWING
    def parse_list(self, response):

        for item in response.xpath("//section/ul/li[@class='hide grid']/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        rented = "".join(response.xpath("//div[@class='sale-method']/span[contains(.,'DEPOSIT TAKEN')]/text()").extract())
        if rented:
            return

        prop = "".join(response.xpath("//div[@class='property-type']/text()").extract())
        if "studio" in prop.lower():
            item_loader.add_value("property_type", "studio")
            item_loader.add_value("room_count", "1")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("real-estate/")[1].split("/")[0].strip())
        item_loader.add_value("external_source", "1stcity_Com_PySpider_australia")
        item_loader.add_xpath("title", "//title/text()")

        item_loader.add_value("address", item_loader.get_collected_values("title")[0].split('::')[1].strip())
        item_loader.add_value("zipcode", item_loader.get_collected_values("address")[0].split(',')[-1].strip())
        item_loader.add_xpath("city", "//div[@class='inner-info-wrapper']/div/h1/text()")

        rent = "".join(response.xpath("//div[@class='sale-method']/span[contains(.,'$')]/text()").extract())
        if rent:
            price = rent.split(" ")[0].replace("\xa0",".").replace(",","").replace(" ","").replace("$","").strip()
            item_loader.add_value("rent", int(float(price))*4)
        item_loader.add_value("currency", "AUD")

        room = "".join(response.xpath("normalize-space(//div[@class='inner-wrapper']/i[@class='bbc-bed-large']/following-sibling::h1/text())").extract())
        if room:
            item_loader.add_value("room_count", room.strip())

        bath = "".join(response.xpath("normalize-space(//div[@class='inner-wrapper']/i[@class='bbc-bath-large']/following-sibling::h1/text())").extract())
        if bath:
            item_loader.add_value("bathroom_count", bath.strip())

        available_date=response.xpath("//div[@class='sale-method']/span[contains(.,'Available')]/text()").get()
        if available_date:
            date2 =  available_date.split(":")[1].strip()
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)

        desc = " ".join(response.xpath("//div[@class='left']/div/span/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        images = [x for x in response.xpath("//section[@id='listing-show-romeo']/div/img/@src").extract()]
        if images:
            item_loader.add_value("images", images)

        floor_plan_images = [x for x in response.xpath("//div[@class='floorplan']//img/@src").extract()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)


        parking = response.xpath("normalize-space(//div[@class='bbc-wrapper']/div[@class='inner-wrapper']/i[@class='bbc-car-large']/following-sibling::h1/text())").extract_first()
        if parking:
            item_loader.add_value("parking", True)

        item_loader.add_xpath("latitude", "substring-before(substring-after(//script/text()[contains(.,'latLng')],'latLng = ['),',')")
        item_loader.add_xpath("longitude", "substring-before(substring-after(substring-after(//script/text()[contains(.,'latLng')],'latLng = ['),', '),']')")

        landlord_name = response.xpath("//div[@class='bottom']/h2/text()").extract_first()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name.strip())
        else:
            item_loader.add_value("landlord_name", "1ST CITY")

        landlord_phone = response.xpath("//div[@class='bottom']/div/a/@href[contains(.,'tel')]").extract_first()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.split(":")[1].strip())
        else:
            item_loader.add_value("landlord_phone", "(02) 8316 4340")


        email = response.xpath("normalize-space(//div[@class='bottom']/div/a/@href[contains(.,'mailto')])").extract_first()
        if email:
            item_loader.add_value("landlord_email", email.split(":")[1].strip())
        else:
            item_loader.add_value("landlord_email", "reception@1stcity.com.au")


        yield item_loader.load_item()
