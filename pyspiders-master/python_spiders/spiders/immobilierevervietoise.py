# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
from ..loaders import ListingLoader
from ..helper import *


class ImmobilierevervietoiseSpider(scrapy.Spider):
    name = "immobilierevervietoise"
    allowed_domains = ["immobilierevervietoise.be"]
    start_urls = (
        "https://immobilierevervietoise.be/biens/a-louer",
    )
    execution_type = "testing"
    country = "belgium"
    locale = "fr"
    thousand_separator = "."
    scale_separator = ","
    external_source="Immobilierevervietoise_PySpider_belgium_fr"

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response, **kwargs):
        for item in response.xpath("//div[@class='property-image']/parent::a/@href").getall():
            follow_url=response.urljoin(item)
            yield scrapy.Request(follow_url,self.parse_detail,)
 
    def parse_detail(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        if "a-louer" not in response.url:
            return 
        item_loader.add_value("external_source", self.external_source)
        external_id=response.xpath("//dt[.='Référence']/following-sibling::dd/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)
        adres=response.xpath("//header[@class='property-title']//figure/text()").get()
        if adres:
            item_loader.add_value("address",adres)

        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)

        zipcode=response.xpath("//header[@class='property-title']//figure//text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.split(",")[-2].split()[0])
            item_loader.add_value("city",zipcode.split(" ")[1])

        rent=response.xpath("//dt[.='Prix']/following-sibling::dd/span/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[0].strip())
        item_loader.add_value("currency","EUR")

        property_type=response.xpath("//dt[.='Type:']/following-sibling::dd/text()").get()
        if property_type:
            if "appartement" in property_type.lower():
                item_loader.add_value("property_type","apartment")
            if "Commerce"==property_type:
                return 
            else:
                item_loader.add_value("property_type","house")

        square_meters=response.xpath("//dt[.='Surface habitable:']/following-sibling::dd/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m")[0].split(".")[0])
        room_count=response.xpath("//dt[.='Chambre:']/following-sibling::dd/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        floor=response.xpath("//dt[.='Etage:']/following-sibling::dd/text()").get()
        if floor:
            item_loader.add_value("floor",floor)
        description=" ".join(response.xpath("//section[@id='description']/div/text() | //section[@id='description']//p//text()").getall())
        if description:
            item_loader.add_value("description",description)
        images=response.xpath("//img[@class='gallery_picture']/@src").getall()
        if images:
            item_loader.add_value("images",images)
        utilities=response.xpath("//li[contains(.,'Charges')]/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split(":")[-1].split("\xa0€")[0])
        landlord_phone=response.xpath("//div[@class='agent-contact-info']//dt[.='Téléphone:']/following-sibling::dd/a/@href").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone",landlord_phone)
        landlord_email=response.xpath("//div[@class='agent-contact-info']//dt[.='Email:']/following-sibling::dd/a/@href").get()
        if landlord_email:
            item_loader.add_value("landlord_email",landlord_email.split("to:")[-1])

        landlord_name = response.xpath("//div[@class='pp--header']/h2/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name",landlord_name)
        # item_loader.add_value("landlord_name", "Immobilière Vervetoise")
        item_loader.add_xpath("landlord_phone","//span[.='phone']/following-sibling::span/text()")
        yield item_loader.load_item()

 

        