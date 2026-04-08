import React from 'react';
import './AboutMePage.css';

/**
 * About the author — replace placeholder copy with your bio, links, and photo if desired.
 */
export default function AboutMePage() {
  return (
    <div className="about-me-page">
      <h1 className="about-me-title">About me</h1>
      <div className="about-me-text-block">
        <p className="about-me-text">
          [Add your bio here: name, program, advisors, research interests, and how to reach you. You
          can also add a photo by placing an image in <code>public/</code> and using an{' '}
          <code>&lt;img&gt;</code> tag.]
        </p>
      </div>
    </div>
  );
}
