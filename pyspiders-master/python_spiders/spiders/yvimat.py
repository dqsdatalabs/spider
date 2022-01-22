# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy, copy, urllib
from ..loaders import ListingLoader
from ..helper import extract_number_only, format_date, remove_white_spaces
from datetime import date
from ..user_agents import random_user_agent


class YvimatSpider(scrapy.Spider):
    name = 'yvimat_be'
    allowed_domains = ['yvimat.be']
    start_urls = ['https://www.yvimat.be/nl-be/te-huur']
    execution_type = 'testing'
    country = 'belgium'
    locale = 'nl'
    api_url = 'https://www.yvimat.be/nl-be/te-huur'
    params = {'pageindex': 1}
    position = 0
    thousand_separator = ' '
    scale_separator = ','

    def start_requests(self):
        start_urls = [self.api_url + "?" + urllib.parse.urlencode(self.params)]
        for url in start_urls:
            yield scrapy.Request(url=url,
                                 callback=self.parse,
                                 meta={'request_url': url,
                                       'params': self.params})

    def parse(self, response, **kwargs):
        for property_url in response.xpath('.//*[(@class="card__image")]//a/@href').extract():
            yield scrapy.Request(
                url=response.urljoin(property_url),
                callback=self.get_property_details,
                meta={'request_url': response.urljoin(property_url)}
            )

        if len(response.xpath('.//*[(@class="card__image")]//a')) > 0:
            current_page = response.meta["params"]["pageindex"]
            params1 = copy.deepcopy(self.params)
            params1["pageindex"] = current_page + 1
            next_page_url = self.api_url + "?" + urllib.parse.urlencode(params1)
            yield scrapy.Request(
                url=next_page_url,
                callback=self.parse,
                meta={'request_url': next_page_url,
                      'params': params1}
            )

    def get_property_details(self, response):
        property_type = response.xpath('.//td[contains(text(), "Type:")]/../td[2]/text()').extract_first()
        if property_type in ["Appartement", "Woning"]:
            item_loader = ListingLoader(response=response)
            item_loader.add_value('external_link', response.meta.get('request_url'))
            item_loader.add_xpath('external_id', './/td[contains(text(), "Referentie:")]/../td[2]/text()')
            item_loader.add_xpath('title', './/*[contains(@class,"page-header--detail")]//h1//text()')

            prop = response.xpath('//td[contains(text(), "Type:")]/../td[2]/text()').extract_first()
            if "woning" in prop.lower():
                item_loader.add_value('property_type', 'house')
            elif "Appartement" in prop:
                item_loader.add_value('property_type', 'apartment')
            else:
                return

            item_loader.add_xpath('description', './/div[@class="detail--text__container"]/p/text()')
            item_loader.add_xpath('images', './/div[@id="detailSlide"]//a[@class="detail-image"]/@href')
            # item_loader.add_xpath('city', './/*[@class="pub__city"]/text()')
            item_loader.add_xpath('city', './/td[contains(text(), "Adres:")]/../td[2]/text()[2]')
            item_loader.add_xpath('address', './/td[contains(text(), "Adres:")]/../td[2]/text()')
            zipcode = response.xpath('.//td[contains(text(), "Adres:")]/../td[2]/text()[1]').extract_first()
            if zipcode:
                item_loader.add_value('zipcode', zipcode.strip().split(" ")[-1])
            geo = " ".join(response.xpath("substring-after(//script[@type='application/ld+json']/text()[contains(.,'latitude')],'geo')").extract())
            if geo:
                lat = geo.split("latitude")[1].split(",")[0].replace('":',"").strip()
                lng = geo.split("longitude")[1].split(",")[0].replace('":',"").strip()
                item_loader.add_value('latitude', lat)
                item_loader.add_value('longitude', lng)


            item_loader.add_xpath('rent_string', './/*[@class="detail__price"]/text()')

            item_loader.add_xpath('room_count', './/*[contains(@class,"feature__item--bed")]//dd/text()')
            item_loader.add_xpath('bathroom_count', './/*[contains(@class,"feature__item--bath")]//dd/text()')
            item_loader.add_xpath('square_meters', './/*[contains(@class,"feature__item--habitable")]//dd/text()')
            available_date = response.xpath('.//td[contains(text(), "Beschikbaar vanaf:")]/../td[2]/text()').extract_first()
            if available_date and available_date == "Onmiddellijk":
                available_date = date.today().strftime("%d/%m/%Y")
            if available_date:
                item_loader.add_value('available_date', format_date(available_date, "%d/%m/%Y"))
            item_loader.add_xpath('floor', './/td[contains(text(), "Op verdieping:")]/../td[2]/text()')

            # pets_allowed
            # https://www.yvimat.be/nl-be/detail/huren-commerciele-winkel-winkelruimte-otegem/5313419
            pets_allowed = response.xpath('.//td[contains(text(), "Huisdieren toegelaten:")]/../td[2]/text()').extract_first()
            if pets_allowed and pets_allowed == "Ja":
                item_loader.add_value('pets_allowed', True)
            elif pets_allowed:
                item_loader.add_value('pets_allowed', False)

            # parking
            # https://www.yvimat.be/nl-be/detail/huren-appartement-zwevegem/5303700
            parking = response.xpath('.//td[contains(text(), "Garage:") or contains(text(), "Parking:")]/../td[2]/text()').extract_first()
            if parking and parking in ['0', 'Neen']:
                item_loader.add_value('parking', False)
            elif parking:
                item_loader.add_value('parking', True)

            # elevator
            # https://www.yvimat.be/nl-be/detail/huren-appartement-zwevegem/5303700
            elevator = response.xpath('.//td[contains(text(), "Lift:")]/../td[2]/text()').extract_first()
            if elevator and elevator == "Ja":
                item_loader.add_value('elevator', True)
            elif elevator:
                item_loader.add_value('elevator', False)

            # terrace
            # https://www.yvimat.be/nl-be/detail/huren-appartement-avelgem/5433411
            terrace = response.xpath('.//td[contains(text(), "Terras:")]/../td[2]/text()').extract_first()
            if terrace and terrace in ['0', 'Neen']:
                item_loader.add_value('terrace', False)
            elif terrace:
                item_loader.add_value('terrace', True)

            item_loader.add_xpath('utilities', './/td[contains(text(), "Gemeenschappelijke")]/../td[2]/text()')
            item_loader.add_xpath('utilities', './/td[contains(text(), "Totale kosten:")]/../td[2]/text()')
            item_loader.add_xpath('energy_label', './/td[contains(text(), "EPC Index:")]/../td[2]/text()')

            item_loader.add_value('landlord_name', "Yvimat BV")
            item_loader.add_value('landlord_email', "info@yvimat.be")
            item_loader.add_value('landlord_phone', "056 75 76 79")

            self.position += 1
            item_loader.add_value('position', self.position)
            item_loader.add_value("external_source", "Yvimat_PySpider_{}_{}".format(self.country, self.locale))
            yield item_loader.load_item()
