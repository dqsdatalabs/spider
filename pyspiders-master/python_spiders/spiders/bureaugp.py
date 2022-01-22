# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from ..loaders import ListingLoader
import js2xml
import lxml.etree
from parsel import Selector

class BureaugpSpider(scrapy.Spider):
    name = "bureaugp"
    allowed_domains = ["bureaugp.be"]
    start_urls = (
        'https://www.bureaugp.be/?p=listerBiens&action=L',
    )
    execution_type = 'testing'
    country = 'belgium'
    locale ='fr'
  
    def parse(self, response, **kwargs):
      
        for item in response.xpath("//div[@class='listing']"):
            prop_type = ""
            property_type = item.xpath(".//h4/span[@class='pull-right']/text()").get()
          
            follow_url = item.xpath("./a/@href").get() 
            if property_type: 
                prop_type = get_p_type_string(property_type)
            if prop_type:
                yield scrapy.Request(response.urljoin(follow_url), callback=self.get_details, meta={'property_type':prop_type})
     
    def get_details(self, response, **kwargs):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
        item_loader.add_css("address", "div:contains('Situation') + div b *::text")
        # item_loader.add_css("rent_string", "p:contains('Loyer') u::text")
        item_loader.add_value("property_type",  response.meta["property_type"])
        rent = response.xpath("//b[contains(.,'Loyer:') or contains(.,'à partir de')]/text()").get()
        if rent:
            item_loader.add_value("rent", "".join(filter(str.isnumeric, rent)))
            item_loader.add_value("currency", "EUR")
        if not item_loader.get_collected_values("address"):
            address = ' '.join(response.xpath("//div[@class='panel-body']/p[@class='font-sm']//text()").extract())
            if address:
                item_loader.add_value("address", address.strip())
        city = ' '.join(response.xpath("//div[@class='panel-body']/p[@class='font-sm']//text()[last()]").extract())
        if city: 
            item_loader.add_value("city", " ".join(city.strip().split(" ")[1:]))
            item_loader.add_value("zipcode", city.strip().split(" ")[0])
        

        item_loader.add_xpath("room_count", ".//span[contains(./i/@class, 'la-bed')]/text()")
        item_loader.add_xpath("bathroom_count", ".//span[contains(./i/@class, 'la-bathroom')]/text()")

        item_loader.add_css("utilities", "p:contains('Charges') u *::text")
        item_loader.add_xpath("square_meters", ".//span[contains(./i/@class, 'la-arrows')]/text()")
        item_loader.add_css("landlord_name", "div:nth-child(2) > .col-md-4 > .panel > div:nth-child(2) > font > b *::text")
        landlord_string = response.css("div:nth-child(2) > .col-md-4 > .panel > div:nth-child(2) > font::text").extract()
        if len(landlord_string) >= 1:
            item_loader.add_value("landlord_email", landlord_string[0])
        if len(landlord_string) > 2:
            item_loader.add_value("landlord_phone", landlord_string[1])

        if not item_loader.get_collected_values("landlord_phone"):
            item_loader.add_xpath("landlord_phone", "//div[contains(@class,'agent')]/div[last()]//font/text()[last()]")

        external_id = ''.join(response.css("div:nth-child(5) > .col-md-12 > .panel > .panel-heading > u *::text").extract())
        if external_id:
            item_loader.add_value("external_id", external_id.split(':')[-1])
        item_loader.add_css("title", "div:nth-child(5) > .col-md-12 > .panel > .panel-heading > .resutls-table-title *::text")
        item_loader.add_css("description", "div:nth-child(5) > .col-md-12 > .panel > .panel-body > .text-descr *::text")
        item_loader.add_css("energy_label", ".details > div:nth-child(4) > .panel > .uu > p:nth-child(1) > u *::text")
        parking = response.css("div:nth-child(2) > .col-md-4 > .panel > div:nth-child(1) > .icofeature > span:nth-child(4) *::text").extract_first()
        if parking: 
            item_loader.add_value("parking", True)
        furnished = response.xpath("//span[@class='resutls-table-title']//text()[contains(.,'garnie')]").extract_first()
        if furnished:
            item_loader.add_value("furnished", True)
        item_loader.add_css("images", "div.galwi-pic::attr(src)")
        javascript = response.xpath('.//script[contains(text(), "showMap")]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            xml_selector = Selector(text=xml)
            item_loader.add_value('latitude', xml_selector.xpath('.//identifier[@name="showMap"]/../../arguments/number/@value').extract()[0])
            item_loader.add_value('longitude', xml_selector.xpath('.//identifier[@name="showMap"]/../../arguments/number/@value').extract()[1])
 
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("appartement" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "maison" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None