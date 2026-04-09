import React from 'react';
import './AboutMePage.css';

export default function AboutMePage() {
  return (
    <div className="about-me-page">
      <h1 className="about-me-title">About me</h1>
      <div className="about-me-text-block">
        <p className="about-me-text">
          Hi! Thank you so much for visiting the CADENCE platform. My name is Claire Goldberg and
          this platform – CADENCE – is my undergraduate thesis at Princeton University. The project
          was advised by Professor Chris Greig of the Andlinger Center for Energy and the
          Environment.
        </p>
        <p className="about-me-text">
          In many ways, CADENCE represents the culmination of my time here at Princeton. As I worked
          to figure out what it was that I wanted to study, the one thing I always knew were what
          types of questions I liked to ask. Specifically, I always liked to think about the
          environment – not from an ecology perspective per se, but from a pragmatic one. I liked
          anything that brought me to the “so what” realm of environmental questions – those that
          focused on what we were actually going to do to solve things. This led me to study
          environmental protection and the role policy interventions can and should play in this
          challenge. This is how I wound up in the High Meadows Environmental Institute (HMEI) and
          the Andlinger Center at Princeton, conducting a minor in Environmental Studies.
        </p>
        <p className="about-me-text">
          At the same time, I was also drawn to mathematical questions: how can math help us explain
          the interactions around us and be a tool to solve problems. This applications-based side
          of math led me towards the Computer Science major at Princeton. There, I built out my tool
          box to explore and address these environmental and policy questions through a
          computational lens. The technical exploration enabled by the COS department had me hooked,
          and I was thrilled to declare the COS major my Sophomore year.
        </p>
        <p className="about-me-text">
          CADENCE is, as I said, a true culmination of my work and ideas at Princeton. It puts
          computer science in direct conversation with environmental protection and policy. Even
          more so, it answers a crucial “so what” of the energy transition: how are we actually
          going to transition to net-zero? Answering these so-what environmental questions with
          computational skills is, in my mind, the most optimistic way to think about the
          environment. The pragmatism it demands offers a sense of relief – a sort of an antidote –
          to the sense of anxiety and peril posed by the massive challenge at hand.
        </p>
        <p className="about-me-text">
          CADENCE, then, stands not only as a contribution to academia, policy, and investments. It
          also stands as a contribution to the general audience who cares and, like me, worries
          about the environment. Through this tool, we can imagine the very near, very real path
          towards a net-zero future.
        </p>
        <p className="about-me-text">
          With any questions, you can email me at{' '}
          <a href="mailto:clairesgoldberg@gmail.com">clairesgoldberg@gmail.com</a>. The code base for
          this CADENCE platform is hosted publicly on github, at{' '}
          <a
            href="https://github.com/clairegoldberg2004/CADENCE_Platform"
            target="_blank"
            rel="noopener noreferrer"
          >
            https://github.com/clairegoldberg2004/CADENCE_Platform
          </a>
          .
        </p>
      </div>
    </div>
  );
}
