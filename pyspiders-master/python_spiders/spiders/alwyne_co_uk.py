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
    name = 'alwyne_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    external_source = 'Addressre_PySpider_belgium_nl'
    def start_requests(self):

        data = {
            "sortorder": "price-desc",
            "RPP": "12",
            "OrganisationId": "d48fea90-d631-468c-938b-7d5f5542e4fd",
            "WebdadiSubTypeName": "Rentals",
            "Status": "{2a50fde6-8f09-4d01-9514-7a856e206d04},{e9617465-c405-4b6a-abc9-fdbfc499145c},{59c95297-2dca-4b55-9c10-220a8d1a5bed}",
            "includeSoldButton": "true",
            "page": "1",
            "incsold": "true",
        }

        yield FormRequest(
            "https://www.alwyne.co.uk/api/set/results/list",
            formdata=data,
            callback=self.jump,
            meta={ "base_url":"https://www.alwyne.co.uk/api/set/results/list"}
        )


    def jump(self,response):
        # url = "https://www.alwyne.co.uk/let/property-to-let"
        # yield Request(url,callback=self.jump)
        page = response.meta.get("page", 2)
        seen = False

        # organisationId =  response.xpath("normalize-space(//form/input[@name='organisationId']/@value)").extract_first()
        # print("organisationId = > ",organisationId)
        # status = response.xpath("//div[@id='property-results']/@data-status").extract_first()
        # print("status = > ",status)

        for item in response.xpath("//div[@class='carousel-wrap']/div"):
            url = response.urljoin(item.xpath("./@data-url").get())
            print(url)
            # status = item.xpath(".//div[contains(@class,'roperty-status')]/span/text()").get()
            # print(status)
            # if status and ("agreed" in status.lower()):
            #     continue
            yield Request(url, callback=self.populate_item)
            seen = True

        if page == 2 or seen:

            data = {
                "sortorder": "price-desc",
                "RPP": "12",
                "OrganisationId": "d48fea90-d631-468c-938b-7d5f5542e4fd",
                "WebdadiSubTypeName": "Rentals",
                "Status": "{2a50fde6-8f09-4d01-9514-7a856e206d04},{e9617465-c405-4b6a-abc9-fdbfc499145c},{59c95297-2dca-4b55-9c10-220a8d1a5bed}",
                "includeSoldButton": "true",
                "page": f"{page}",
                "incsold": "true",
            }

            yield FormRequest(
                "https://www.alwyne.co.uk/api/set/results/list",
                formdata=data,
                callback=self.jump,
                meta={"page":page+1}
            )




    # 2. SCRAPING level 2
    def populate_item(self, response):

        item_loader = ListingLoader(response=response)

        status = " ".join(response.xpath("//h2[@class='color-primary mobile-left']/text()").extract())
        if status and ("agreed" in status.lower()):
            return

        property_type = status
        if get_p_type_string(property_type):
            item_loader.add_value("property_type", get_p_type_string(property_type))
        else: return
        item_loader.add_value("external_link", response.url.split("?")[0])
        item_loader.add_value("external_source", "Alwyne_Co_PySpider_united_kingdom")
        item_loader.add_xpath("title", "//div/h2/text()")
        address = response.xpath("//div/h2/text()").extract_first()
        if address:
            item_loader.add_value("address", address.replace("|",",").strip())
            if "|" in address:
                item_loader.add_value("zipcode", address.split("|")[-1].strip())

        rent = " ".join(response.xpath("//div[contains(@class,'property-price')]/h2/span[@class='nativecurrencyvalue']/text() | //div[contains(@class,'property-price')]/h2/text()").extract())
        if rent:
            if "per week" in rent.lower():
                rent = " ".join(response.xpath("//div[contains(@class,'property-price')]/h2/span[@class='nativecurrencyvalue']/text()").getall())
                item_loader.add_value("rent", str(int(float(rent)) * 4))
            else:
                rent = " ".join(response.xpath("//div[contains(@class,'property-price')]/h2/span[@class='nativecurrencyvalue']/text()").getall())
                item_loader.add_value("rent", rent.strip())

        item_loader.add_value("currency", 'GBP')

        room_count = response.xpath("//div[contains(@class,'col-md-12')]//li[@class='FeaturedProperty__list-stats-item']/span/text()").extract_first()
        if room_count:
            if "Studio" in room_count:
                item_loader.add_value("room_count","1")
            else:
                item_loader.add_value("room_count",room_count)
        bathroom_count = response.xpath("//div[span[@class='icon-bath']]/text()").extract_first()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip(" ")[0])

        images = [x.split("url(")[1].split(")")[0] for x in response.xpath("//div[@id='imageViewerCarousel']/div/div/@style").extract()]
        if images:
                item_loader.add_value("images", images)
        balcony = response.xpath("//div[@id='details']/h3[.='Features']/following-sibling::text()[contains(.,'Balcony')]").extract_first()
        if balcony:
            item_loader.add_value("balcony", True)

        parking = response.xpath("//div[span[@class='icon-parking']]/text()").extract_first()
        if parking:
            if "no" in parking.lower():
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)

        desc = " ".join(response.xpath("//section[@id='description']/p/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
        latitude = response.xpath("substring-before(substring-after(//div[@class='details-streetview-wrap']/iframe/@src,'cbll='),',')").get()
        if latitude:
            item_loader.add_value("latitude", latitude.strip())
        longitude = response.xpath("substring-after(substring-after(//div[@class='details-streetview-wrap']/iframe/@src,'cbll='),',')").get()
        if longitude:
            item_loader.add_value("longitude", longitude.split("&")[0].strip())

        item_loader.add_value("landlord_name", "Alwyne Estates")
        item_loader.add_value("landlord_phone", "0207 359 3191")
        item_loader.add_value("landlord_email", "info@alwyne.co.uk")
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and "kamer" in p_type_string.lower():
        return "room"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "bovenwoning" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "cottage" in p_type_string.lower() or "terrace" in p_type_string.lower() or "gezinswoning" in p_type_string.lower() or "benedenwoning" in p_type_string.lower() or "woonboot" in p_type_string.lower()):
        return "house"
    else:
        return None