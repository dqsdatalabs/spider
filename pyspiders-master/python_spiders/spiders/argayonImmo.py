# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
from ..loaders import ListingLoader
from ..helper import *
from scrapy import Request,FormRequest


class ArgayonimmoSpider(scrapy.Spider):
    name = "argayonImmo"
    allowed_domains = ["argayon-immo.be"]
    start_urls = ("http://www.argayon-immo.be/",)
    execution_type = "testing"
    country = "belgium"
    locale = "fr"
    thousand_separator = "."
    scale_separator = ","

    def start_requests(self):

        start_urls = [
            {"url": "https://www.argayon-immo.be/fr-BE/List/PartialListEstate/7?EstatesList=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.Estate%5D&EstateListForNavigation=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.Estate%5D&EstateRef=&SelectedType=System.Collections.Generic.List%601%5BSystem.String%5D&Categories=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&MinPrice=&MaxPrice=&Rooms=0&ListID=7&SearchType=ToRent&SearchTypeIntValue=0&SelectedCities=&Cities=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&SortParameter=Date_Desc&Furnished=False&InvestmentEstate=False&GroundMinArea=&CurrentPage=0&MaxPage=1&EstateCount=17&SoldEstates=False&List=Webulous.Immo.DD.CMSEntities.EstateListContentObject&Page=Webulous.Immo.DD.CMSEntities.Page&ContentZones=System.Collections.Generic.List%601%5BWebulous.Immo.DD.CMSEntities.ContentZone%5D&DisplayMap=False&MapMarkers=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.MapMarker%5D&MapZipMarkers=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.MapZipMarker%5D&EstateTotalCount=0&isMobileDevice=False&SelectedCountries=&Countries=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&CountrySearch=Undefined", "property_type": "apartment"},
	        {"url": "https://www.argayon-immo.be/fr-BE/List/PartialListEstate/7?EstatesList=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.Estate%5D&EstateListForNavigation=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.Estate%5D&EstateRef=&SelectedType=System.Collections.Generic.List%601%5BSystem.String%5D&Categories=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&MinPrice=&MaxPrice=&Rooms=0&ListID=7&SearchType=ToRent&SearchTypeIntValue=0&SelectedCities=&Cities=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&SortParameter=Date_Desc&Furnished=False&InvestmentEstate=False&GroundMinArea=&CurrentPage=0&MaxPage=1&EstateCount=17&SoldEstates=False&List=Webulous.Immo.DD.CMSEntities.EstateListContentObject&Page=Webulous.Immo.DD.CMSEntities.Page&ContentZones=System.Collections.Generic.List%601%5BWebulous.Immo.DD.CMSEntities.ContentZone%5D&DisplayMap=False&MapMarkers=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.MapMarker%5D&MapZipMarkers=System.Collections.Generic.List%601%5BWebulous.Immo.DD.WEntities.MapZipMarker%5D&EstateTotalCount=0&isMobileDevice=False&SelectedCountries=&Countries=System.Collections.Generic.List%601%5BSystem.Web.Mvc.SelectListItem%5D&CountrySearch=Undefined", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse_js,
                            meta={'property_type': url.get('property_type'),
                            })


    def parse_js(self, response):
        property_type = response.meta.get("property_type")
        if response.xpath('//a[contains(text(), "next page")]'):
            next_link = response.urljoin(response.xpath('//a[contains(text(), "next page")]/@href').extract_first())
            yield scrapy.Request(
                url=next_link, callback=self.parse_js, dont_filter=True,  meta={'property_type': response.meta.get('property_type')}
            )
        for node in response.xpath(".//a[@class='estate-thumb' and @href]"):
            link = node.xpath("@href")
            item = {}
            item["room_count"] = node.xpath(
                ".//span[@class='estate-picto' and i[contains(@class,'fw-bedroom')]]//text()"
            ).get()

            item["bathroom_count"] = node.xpath(
                ".//span[@class='estate-picto' and i[contains(@class,'fw-bathroom')]]//text()"
            ).get()
            item["city"] = node.xpath(".//span[@class='estate-text-strong']//text()").get()

            item["rent_string"] = node.xpath(".//span[@class='estate-text-emphasis']/text()").get()
            

            yield response.follow(
                link.get(),
                self.parse_detail,
                cb_kwargs=dict(item=item),
                meta={'property_type': response.meta.get('property_type')},
            )

    def parser_map(self, item_loader, response):
        geos = re.findall("\d+[.]\d{5,}", response.text)
        if len(geos) == 2:
            item_loader.add_value("latitude", geos[0])
            item_loader.add_value("longitude", geos[1])

    def parse_detail(self, response, item):
        item_loader = ListingLoader(response=response)

        prop_type = ""
        prop =  "".join(response.xpath("//tr//th[contains(.,'Cat')]/following-sibling::td/text()").extract())
        if prop:
            if prop == "appartement":
                prop_type= "apartment"
            elif prop == "penthouse":
                prop_type= "house"
            elif prop == "studio":
                prop_type= "studio"
            else:
                return

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", prop_type)
        property_type = response.meta.get("property_type")
        item_loader.add_value(
            "external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale)
        )
        for k, v in item.items():
            item_loader.add_value(k, v)
        self.parser_map(item_loader, response)
        main_block = ".//div[@id='site-main']"
        item_loader.add_xpath("title", ".//head//meta[@property='og:title']/@content")



        item_loader.add_value(
            "zipcode", response.xpath(f"{main_block}//div[@class='estate-feature']/p[1]/text()").re("\d{4}")
        )
        item_loader.add_xpath("address", f"{main_block}//div[@class='estate-feature']/p[1]/text()")
        item_loader.add_xpath("property_type", property_type)
        item_loader.add_xpath("description", f"{main_block}//div[h2[.='Description']]/p//text()")

        item_loader.add_xpath("images", ".//a[contains(@rel,'group')]/@href")
        item_loader.add_xpath("floor_plan_images", ".//div[contains(@class,'estate-files')]//a/@href")

        parking = response.xpath("//table//tr[th[.='Parking']]/td/text()").extract_first()
        if parking:
            if "Oui" in parking:
                item_loader.add_value("parking", True)
            elif "Non" in parking:
                item_loader.add_value("parking", False)

        # self.get_by_keywords(
        #     item,
        #     self.get_from_detail_panel(response.xpath(".//*[@class='estate-table']")),
        # )

        self.get_from_detail_panel(
            " ".join(response.xpath(f".//*[@class='estate-table']//tr[td[contains(.,'Non')]]/th//text()").getall()),
            item_loader,
            bool_value=False,
        )
        self.get_from_detail_panel(
            " ".join(
                response.xpath(f".//*[@class='estate-table']//tr[td[not(contains(.,'Non'))]]/th//text()").getall()
            ),
            item_loader,
        )
        self.get_general(item_loader)
        contact = ".//div[@class='division estate-contact']"
        item_loader.add_xpath("landlord_phone", f"{contact}/p/text()[2]")

        item_loader.add_xpath("landlord_name", f"{contact}/h3/text()")

        item_loader.add_xpath("landlord_email", f"{contact}/p/a/text()")
        yield item_loader.load_item()

    def get_general(self, item_loader):
        keywords = {
            "external_id": "Référence",
            "floor": "Étages (nombre)",
            "utilities": "Charges",
            # "available_date": "Date de disponibilité",
            "room_count": "Nombre de chambres",
            "bathroom_count": "Nombre de salle de bain",
            "square_meters": "Surface habitable",
        }
        for k, v in keywords.items():
            item_loader.add_xpath(k, f'.//*[@class="estate-table"]//tr[th[contains(.,"{v}")]]/td//text()')

    def get_from_detail_panel(self, text, item_loader, bool_value=True):
        """check all keywords for existing"""
        keywords = {
            "parking": [
                "parking",
                "garage",
                "car",
                "aantal garage",
            ],
            "balcony": [
                "balcon",
                "nombre de balcon",
                "Nombre d",
                "balcony",
                "balcon arrière",
            ],
            "pets_allowed": ["animaux"],
            "furnished": ["meublé", "appartement meublé", "meublée"],
            "swimming_pool": ["piscine"],
            "dishwasher": ["lave-vaisselle"],
            "washing_machine": ["machine à laver", "lave linge"],
            "terrace": ["terrasse", "terrasse de repos", "terras"],
            "elevator": ["ascenseur", "elevator"],
        }

        value = remove_white_spaces(text).casefold()
        for k, v in keywords.items():
            if any(s in value for s in v):
                item_loader.add_value(k, bool_value)
