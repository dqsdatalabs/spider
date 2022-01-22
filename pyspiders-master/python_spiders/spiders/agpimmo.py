# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader
from ..helper import *


class AgpimmoSpider(scrapy.Spider):
    """
    all images got 404 error
    """
    name = "agpimmo"
    allowed_domains = ["agpimmo.be"]
    start_urls = (
        "http://www.agpimmo.be/index.php?action=list&menuid=0&ctypmandatmulti%5B%5D=l&ctypmetamulti%5B%5D=appt&mprixmax=&lcp=&cbien=&search=",
        "http://www.agpimmo.be/index.php?action=list&menuid=0&ctypmandatmulti%5B%5D=l&ctypmetamulti%5B%5D=mai&mprixmax=&lcp=&cbien=&search=",
    )
    execution_type = "testing"
    country = "belgium"
    locale = "fr"
    thousand_separator = "."
    scale_separator = ","

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response, **kwargs):
        old = response
        response = response.xpath("//html")
        for link in response.xpath(".//div[@class='PostContent']//div[@class='Post']//div[@class='Post']"):
            yield old.follow(
                old.url.split("index")[0] + link.xpath(".//a/@href").get(),
                self.parse_detail,
                cb_kwargs=dict(
                    property_type="house" if "mai" in old.url else "apartment",
                    city=link.xpath(".//div[@class='PostHeaderIcons metadata-icons']/a[2]/text()").get(),
                ),
            )

    def parse_detail(self, response, property_type, city):

        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("city", city)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value(
            "external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale)
        )
        main_block = "//html//div[@class='PostContent']//div[@class='PostContent']"
        stats = f"{main_block}//div[h2/text()[contains(.,'Données principales')]]"
        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address", address.strip("-"))
            
        item_loader.add_xpath("description", "//html//head//meta[@name='description']/@content")
        item_loader.add_xpath("title", f"{main_block}//h1/text()")
        item_loader.add_xpath("external_id", "//html//input[@id='cbien']/@value")
        item_loader.add_xpath("rent", f"{stats}//li//text()[contains(.,'Prix')]")
        item_loader.add_value("room_count", response.xpath(f"{stats}//li[contains(.,'Chambres')]//text()").re(r"\d+"))
        item_loader.add_value(
            "bathroom_count", response.xpath(f"{stats}//li[contains(.,'Salles de bains')]/text()").re(r"\d+")
        )
        item_loader.add_xpath("square_meters", f"{stats}//li//text()[contains(.,'Surface habitable')]")
        if response.xpath(f"{stats}//li[contains(.,'Terrasse:')]//text()").get():
            item_loader.add_value("terrace", True)

        bathroom_count = "".join(response.xpath("//ul/li[contains(.,'Salle de bains')]/text()").getall())
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(" ")[0].strip())

        images = [response.urljoin(x) for x in response.xpath("//div[@class='items']/a/img/@src").extract()]
        if images:
                item_loader.add_value("images", images)
        item_loader.add_value("utilities", response.xpath(f"{stats}//li[contains(.,'Charges:')]//text()").re(r"\d+"))
        item_loader.add_value("landlord_phone", "00 32 2 374 51 61")
        item_loader.add_value("landlord_name", "AGP Immobilière")
        item_loader.add_value("currency", "EUR")
        yield item_loader.load_item()