# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader import ItemLoader
from scrapy.loader.processors import TakeFirst, MapCompose
from scrapy import Spider
from scrapy import Request
from python_spiders.items import ListingItem
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
from datetime import datetime
import dateparser


class MySpider(Spider):
    name = "noare_be"
    execution_type = 'testing'
    country = 'belgium'
    locale = 'en'
    
    def start_requests(self):
        start_urls = [
            {"url": "https://www.noa-re.be/en/for-rent/apartment/", "property_type": "apartment"},
            {"url": "https://www.noa-re.be/en/for-rent/house/", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                        callback=self.parse,
                        meta={'property_type': url.get('property_type')})

    def parse(self, response):
        url = response.xpath("//div[@class='prop-container']/h3/a")
        for item in url:
            if "Not found what you were looking for?" not in item.xpath("./text()").extract_first():
                url = item.xpath("./@href").extract_first()
                yield response.follow(url, self.populate_item, meta={"property_type": response.meta.get("property_type")})

        # 2. PAGINATION
        next_page_urls = response.css(
            "div.pagination a::attr(href)"
        ).extract()  # pagination("next button") <a> element here
        for next_page_url in next_page_urls:
            yield response.follow(response.urljoin(next_page_url), self.parse, meta={"property_type": response.meta.get("property_type")})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Noare_PySpider_" + self.country + "_" + self.locale)
        prop = response.meta["property_type"]
        item_loader.add_value("external_link", response.url)
        title = response.xpath("//div[@class='container']//h3//text()").extract_first()
        item_loader.add_value("title",title)

        square_meters = response.xpath("//div[@class='col-1-3 second']//dl/div[dt[.='Living area']]/dd").extract_first()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m²")[0])

        item_loader.add_xpath("description","//div[@id='desc']/div[@itemprop='description']//text()")
        price = response.xpath("//span[@itemprop='price']/text()").extract_first()
        if price:
            item_loader.add_value("rent", price.split("€")[0])
            item_loader.add_value("currency", "EUR")

        utilities = response.xpath(
            "normalize-space(//dl/div[dt[.='Costs']]/dd/span/text()[1])"
        ).extract_first()
        if utilities:
            item_loader.add_value(
                "utilities", utilities.split("€")[1].split("monthly")[0]
            )

        ref = response.xpath(
            "//div[@class='container']//h4/span/text()"
        ).extract_first()
        if ref:
            item_loader.add_value("external_id", ref.split("Ref.:")[1])

        address = "".join(
            response.xpath("//div[@id='address']/address/span[2]/text()").extract()
        )
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("zipcode", split_address(address, "zip"))
            item_loader.add_value("city", split_address(address, "city"))

        item_loader.add_value("property_type", prop)
        room = response.xpath("//div[@class='toolbar']/span[i[contains(.,'Room')]]/text()").extract_first()
        if room:
            item_loader.add_value("room_count", room)
        elif not room and "Studio" in title:
            item_loader.add_value("room_count", "1")

        item_loader.add_xpath("bathroom_count", "//div[@class='toolbar']/span[i[contains(@class,'bath')]]/text()")
        available_date = " ".join(
            response.xpath(
                "//div[@class='col-1-3 third']//dl/div[dt[.='Available']]/dd/span/text()[2]"
            ).extract()
        )
        if available_date:
            if (
                available_date != "Tbd with the owner"
                or available_date != "Immediately"
            ):
                date_parsed = dateparser.parse(
                    available_date, date_formats=["%d %B %Y"]
                )
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

        item_loader.add_xpath(
            "energy_label",
            "normalize-space(//div[contains(@class,'epc-value')]/span/text())",
        )

        floor = "".join(
            response.xpath(
                "//div[@class='col-1-3 first']//dl/div[dt[.='Floor']]/dd//text()"
            ).extract()
        )
        item_loader.add_value("floor", floor.strip())
        images = [
            response.urljoin(x)
            for x in response.xpath("//div[@id='property-images']//a/@href").extract()
        ]
        item_loader.add_value("images", images)

        terrace = response.xpath(
            "//div[@class='col-1-3 first']//dl/div[dt[.='Terrace 1']]/dd//text()"
        ).get()
        if terrace:
            item_loader.add_value("terrace", True)

       
        desc = " ".join(
            response.xpath("//div[@itemprop='description']//text()").extract()
        ).strip()
        if desc:
            if "dishwasher" in desc:
                item_loader.add_value("dishwasher", True)
            if "balcony" in desc:
                item_loader.add_value("balcony", True)
            if "washing machine" in desc:
                item_loader.add_value("washing_machine", True)

        parking = response.xpath(
            "normalize-space(//div[@class='col-1-3 first']//dl/div[dt[.='Parking inside' or .='Garages']]/dd)"
        ).get()
        if parking:
            if parking == "Yes":
                item_loader.add_value("parking", True)
            elif parking == "No":
                item_loader.add_value("parking", False)
        garage = response.xpath("//div[@class='toolbar']/span[i[contains(.,'Garages')]]/text()").extract_first()
        if garage:
            item_loader.add_value("parking", True)
        
        furnished = response.xpath(
            "normalize-space(//div[@class='col-1-3 first']//dl/div[dt[.='Furnished']]/dd)"
        ).get()
        if furnished:
            if furnished == "Yes":
                item_loader.add_value("furnished", True)
            elif furnished == "No":
                item_loader.add_value("furnished", False)

        elevator = response.xpath(
            "normalize-space(//div[@class='col-1-3 first']//dl/div[dt[.='Elevator']]/dd)"
        ).get()
        if elevator:
            if elevator == "Yes":
                item_loader.add_value("elevator", True)
            elif elevator == "No":
                item_loader.add_value("elevator", False)
        swimming_pool = response.xpath(
            "normalize-space(//div[@class='col-1-3 first']//dl/div[dt[.='Pool']]/dd)"
        ).get()
        if swimming_pool:
            if swimming_pool == "Yes":
                item_loader.add_value("swimming_pool", True)
            elif swimming_pool == "No":
                item_loader.add_value("swimming_pool", False)

        phone = response.xpath('//a[@class="no-style"]/text()').get()
        if phone:
            item_loader.add_value("landlord_phone", phone)
        item_loader.add_value("landlord_name", "NOA real estate")
        item_loader.add_value("landlord_email", "info@noa-re.be")
        
        coordinate = response.xpath("//script[contains(.,'lat') and contains(.,'lng')]/text()").extract_first()
        if coordinate:
            lat = coordinate.split("lat =")[1].split(";")[0].strip()
            lng = coordinate.split("lng =")[1].split(";")[0].strip()
            item_loader.add_value("latitude", lat)
            item_loader.add_value("longitude", lng)
        yield item_loader.load_item()


def split_address(address, get):
    # temp = address.split(" ")[0]
    zip_code = "".join(filter(lambda i: i.isdigit(), address))
    city = address.split(" ")[-1]

    if get == "zip":
        return zip_code
    else:
        return city
